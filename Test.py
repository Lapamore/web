import time
import os
import numpy as np
import pandas as pd
import clickhouse_connect
import duckdb
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- Параметры тестирования ---
SIZES = [100_000, 500_000, 1_000_000, 2_000_000]  # Увеличим для наглядности
DIM = 128                           # Размерность векторов
K = 10                              # Количество ближайших соседей для поиска
NUM_REQUESTS_THROUGHPUT = 1000      # Количество запросов для теста пропускной способности
NUM_THREADS = 8                     # Увеличим число потоков для большей нагрузки
HNSW_M = 16                         # Параметр M для HNSW индекса
HNSW_EF_SEARCH = 64                 # Параметр efSearch для HNSW (точность/скорость)

# --- Подключение к базам данных ---
# Замените на ваши реальные данные
try:
    ch_client = clickhouse_connect.get_client(
        host='your-clickhouse-host.com', # <--- ЗАМЕНИТЕ
        port=8443,                      # <--- ЗАМЕНИТЕ (обычно 8443 для https)
        username='your_user',           # <--- ЗАМЕНИТЕ
        password='your_password',       # <--- ЗАМЕНИТЕ
        database="default",
        secure=True                     # <--- Установите True, если используете HTTPS/TLS
    )
    print("ClickHouse connection successful.")
except Exception as e:
    print(f"Failed to connect to ClickHouse: {e}")
    ch_client = None # Не продолжаем, если нет подключения

# DuckDB работает локально, подключение простое
duck_conn = duckdb.connect('vector_benchmark.db')
print("DuckDB connection successful.")


# --- Функции для генерации данных ---
def generate_vectors(n, dim):
    """Генерирует DataFrame с ID и случайными векторами."""
    print(f"Generating {n} vectors of dimension {dim}...")
    ids = np.arange(n, dtype=np.uint32)
    # Генерируем нормализованные векторы, это более реалистичный сценарий
    embs = np.random.random((n, dim)).astype(np.float32)
    embs /= np.linalg.norm(embs, axis=1, keepdims=True)
    return pd.DataFrame({'id': ids, 'embedding': embs.tolist()})

# --- Функции для ClickHouse ---
def test_bulk_insert_ch(df):
    """
    Тестирует высокопроизводительную вставку в ClickHouse.
    Используем client.insert() вместо ручной сборки SQL.
    """
    ch_client.command('DROP TABLE IF EXISTS vectors_ch')
    ch_client.command('''
        CREATE TABLE vectors_ch
        (
            id UInt32,
            embedding Array(Float32)
        )
        ENGINE = MergeTree ORDER BY id
    ''')
    t0 = time.time()
    # Это правильный и быстрый способ вставки данных
    ch_client.insert_df('vectors_ch', df)
    return time.time() - t0

def test_index_creation_ch():
    """
    Тестирует создание векторного индекса в ClickHouse с правильным синтаксисом.
    """
    # Удаляем старый индекс, если он есть, чтобы тест был чистым
    ch_client.command('ALTER TABLE vectors_ch DROP INDEX IF EXISTS idx_vector_l2')
    
    t0 = time.time()
    # Современный и правильный синтаксис для векторного индекса
    ch_client.command(f'''
        ALTER TABLE vectors_ch
        ADD INDEX idx_vector_l2 embedding TYPE vector_similarity(
            'L2Distance', 'HNSW'
        ) GRANULARITY 1
    ''')
    # Индекс строится в фоне, но для теста нужно дождаться его готовности
    while True:
        status = ch_client.query('''
            SELECT status FROM system.vector_indices
            WHERE table = 'vectors_ch' AND name = 'idx_vector_l2'
        ''').result_rows
        if status and status[0][0] == 'Built':
            break
        print("Waiting for ClickHouse index to build...")
        time.sleep(2)
        
    return time.time() - t0

def test_knn_latency_ch(q_vector):
    """Тестирует задержку одного KNN запроса в ClickHouse."""
    t0 = time.time()
    # L2Distance - это оператор <->
    ch_client.query(
        f"SELECT id FROM vectors_ch ORDER BY L2Distance(embedding, {q_vector}) LIMIT {K}"
    )
    return time.time() - t0

def get_disk_usage_ch():
    """Возвращает размер таблицы и индекса в ClickHouse."""
    res = ch_client.query(
        "SELECT sum(bytes_on_disk) FROM system.parts WHERE table = 'vectors_ch' AND active"
    ).result_rows[0][0]
    return int(res) if res else 0

# --- Функции для DuckDB ---
def test_bulk_insert_duck(df):
    """Тестирует вставку данных в DuckDB."""
    duck_conn.execute('DROP TABLE IF EXISTS vectors_duck')
    t0 = time.time()
    # Самый быстрый способ - создать таблицу напрямую из DataFrame
    duck_conn.execute('CREATE TABLE vectors_duck AS SELECT * FROM tmp', {'tmp': df})
    return time.time() - t0

def test_index_creation_duck():
    """Тестирует создание HNSW индекса в DuckDB."""
    duck_conn.execute("INSTALL vss; LOAD vss;") # Используем VSS расширение, оно более новое
    duck_conn.execute("DROP INDEX IF EXISTS idx_hnsw_duck")
    t0 = time.time()
    # Простой и эффективный синтаксис для создания индекса
    duck_conn.execute(
        f"CREATE INDEX idx_hnsw_duck ON vectors_duck USING HNSW(embedding)"
    )
    return time.time() - t0

def test_knn_latency_duck(q_vector):
    """Тестирует задержку одного KNN запроса в DuckDB."""
    t0 = time.time()
    # Используем функцию distance
    duck_conn.execute(
        f"SELECT id FROM vectors_duck ORDER BY list_distance(embedding, {q_vector}) LIMIT {K}"
    ).fetchall()
    return time.time() - t0

def get_disk_usage_duck():
    """Возвращает размер файла базы данных DuckDB."""
    # Принудительно сбрасываем данные на диск для точного измерения
    duck_conn.execute('CHECKPOINT')
    try:
        return os.path.getsize('vector_benchmark.db')
    except FileNotFoundError:
        return 0

# --- Общая функция для теста пропускной способности ---
def test_knn_throughput(test_func, q_vector):
    """Измеряет QPS (queries per second) в многопоточном режиме."""
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # "Прогрев" пула соединений и JIT-компиляторов
        warmup_futures = [executor.submit(test_func, q_vector) for _ in range(NUM_THREADS * 2)]
        for f in warmup_futures: f.result()
        
        # Основной тест
        t0 = time.time()
        futures = [executor.submit(test_func, q_vector) for _ in range(NUM_REQUESTS_THROUGHPUT)]
        for f in futures:
            f.result()
        elapsed = time.time() - t0
    return NUM_REQUESTS_THROUGHPUT / elapsed

# --- Основной цикл тестирования ---
def main():
    if not ch_client:
        print("Cannot run tests without ClickHouse connection. Exiting.")
        return

    results = []
    
    for n in SIZES:
        print(f"\n{'='*20} TESTING WITH N = {n:,} VECTORS {'='*20}")
        
        # 1. Генерация данных
        df = generate_vectors(n, DIM)
        query_vector = df['embedding'].iloc[np.random.randint(0, n)].tolist()
        
        # 2. Вставка (Bulk Insert)
        print("Running Bulk Insert test...")
        bi_ch = test_bulk_insert_ch(df)
        bi_du = test_bulk_insert_duck(df)
        print(f"  > Bulk Insert Time: ClickHouse: {bi_ch:.2f}s, DuckDB: {bi_du:.2f}s")
        
        # 3. Создание индексов
        print("Running Index Creation test...")
        ic_ch = test_index_creation_ch()
        ic_du = test_index_creation_duck()
        print(f"  > Index Creation Time: ClickHouse: {ic_ch:.2f}s, DuckDB: {ic_du:.2f}s")

        # 4. Задержка (Latency)
        print("Running Latency test...")
        lat_ch = np.mean([test_knn_latency_ch(query_vector) for _ in range(10)]) * 1000
        lat_du = np.mean([test_knn_latency_duck(query_vector) for _ in range(10)]) * 1000
        print(f"  > KNN Latency (avg of 10): ClickHouse: {lat_ch:.2f}ms, DuckDB: {lat_du:.2f}ms")
        
        # 5. Пропускная способность (Throughput)
        print("Running Throughput test...")
        qps_ch = test_knn_throughput(test_knn_latency_ch, query_vector)
        qps_du = test_knn_throughput(test_knn_latency_duck, query_vector)
        print(f"  > Throughput (QPS): ClickHouse: {qps_ch:.0f}, DuckDB: {qps_du:.0f}")
        
        # 6. Использование диска
        print("Measuring disk usage...")
        du_ch = get_disk_usage_ch()
        du_du = get_disk_usage_duck()
        print(f"  > Disk Usage: ClickHouse: {du_ch/1e6:.2f}MB, DuckDB: {du_du/1e6:.2f}MB")
        
        results.append({
            'size': n,
            'bulk_insert_ch': bi_ch, 'bulk_insert_duck': bi_du,
            'index_creation_ch': ic_ch, 'index_creation_duck': ic_du,
            'latency_ch': lat_ch, 'latency_duck': lat_du,
            'qps_ch': qps_ch, 'qps_duck': qps_du,
            'disk_usage_ch': du_ch, 'disk_usage_duck': du_du,
        })

    df_res = pd.DataFrame(results)
    print("\n--- Final Results ---")
    print(df_res)
    
    # --- Построение и сохранение графиков ---
    print("\nGenerating and saving plots...")
    
    def plot_and_save(x, y1, y2, y_label, title, file_name):
        plt.style.use('seaborn-v0_8-whitegrid')
        plt.figure(figsize=(10, 6))
        plt.plot(x, y1, 'o-', label='ClickHouse', color='orangered')
        plt.plot(x, y2, 'o-', label='DuckDB', color='dodgerblue')
        plt.xlabel('Number of Vectors')
        plt.ylabel(y_label)
        plt.title(title)
        plt.legend()
        plt.gca().get_xaxis().set_major_formatter(
            plt.matplotlib.ticker.FuncFormatter(lambda val, p: f'{val/1e6:.1f}M'))
        plt.savefig(f"{file_name}.png", dpi=150, bbox_inches='tight')
        plt.close()

    plot_and_save(df_res['size'], df_res['bulk_insert_ch'], df_res['bulk_insert_duck'],
                  'Time (seconds)', 'Bulk Insert Performance', 'bulk_insert')
    
    plot_and_save(df_res['size'], df_res['index_creation_ch'], df_res['index_creation_duck'],
                  'Time (seconds)', 'Index Creation Time', 'index_creation')
    
    plot_and_save(df_res['size'], df_res['latency_ch'], df_res['latency_duck'],
                  'Latency (ms)', 'KNN Search Latency (k=10)', 'knn_latency')
                  
    plot_and_save(df_res['size'], df_res['qps_ch'], df_res['qps_duck'],
                  'Queries Per Second (QPS)', f'KNN Throughput ({NUM_THREADS} threads)', 'knn_throughput')

    plot_and_save(df_res['size'], df_res['disk_usage_ch']/1e6, df_res['disk_usage_duck']/1e6,
                  'Disk Usage (MB)', 'On-Disk Size (Data + Index)', 'disk_usage')

    print("Plots saved successfully.")


if __name__ == '__main__':
    main()
