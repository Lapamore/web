import time
import os
import numpy as np
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- Параметры тестирования ---
SIZES = [100_000, 500_000, 1_000_000, 2_000_000] # Размеры датасетов
DIM = 128                           # Размерность векторов
K = 10                              # Количество ближайших соседей для поиска
NUM_REQUESTS_THROUGHPUT = 1000      # Количество запросов для теста пропускной способности
NUM_THREADS = 8                     # Количество параллельных потоков

# --- Подключение к DuckDB ---
DB_FILE = 'duckdb_benchmark.db'
if os.path.exists(DB_FILE):
    os.remove(DB_FILE) # Удаляем старый файл для чистого теста
duck_conn = duckdb.connect(DB_FILE)
print("DuckDB connection successful.")


# --- Функции для генерации данных ---
def generate_vectors(n, dim):
    """Генерирует DataFrame с ID и случайными векторами."""
    print(f"Generating {n:,} vectors of dimension {dim}...")
    ids = np.arange(n, dtype=np.uint32)
    embs = np.random.random((n, dim)).astype(np.float32)
    embs /= np.linalg.norm(embs, axis=1, keepdims=True)
    return pd.DataFrame({'id': ids, 'embedding': embs.tolist()})


# --- Функции для тестирования DuckDB ---
def test_bulk_insert(df):
    """Тестирует вставку данных в DuckDB."""
    duck_conn.execute('DROP TABLE IF EXISTS vectors')
    t0 = time.time()
    # Самый быстрый способ - зарегистрировать DataFrame и создать таблицу из него
    duck_conn.register('tmp_df', df)
    duck_conn.execute('CREATE TABLE vectors AS SELECT * FROM tmp_df')
    duck_conn.unregister('tmp_df') # Очищаем временное представление
    return time.time() - t0

def test_index_creation():
    """Тестирует создание HNSW индекса в DuckDB."""
    duck_conn.execute("INSTALL vss; LOAD vss;") # VSS - Vector Similarity Search расширение
    duck_conn.execute("DROP INDEX IF EXISTS idx_hnsw")
    t0 = time.time()
    # Простой и эффективный синтаксис для создания индекса
    duck_conn.execute("CREATE INDEX idx_hnsw ON vectors USING HNSW(embedding)")
    return time.time() - t0

def test_knn_latency(q_vector):
    """Тестирует задержку одного KNN запроса в DuckDB."""
    t0 = time.time()
    duck_conn.execute(
        f"SELECT id FROM vectors ORDER BY list_distance(embedding, {q_vector}) LIMIT {K}"
    ).fetchall()
    return time.time() - t0

def test_knn_throughput(q_vector):
    """Измеряет QPS (queries per second) в многопоточном режиме."""
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Прогрев
        futures = [executor.submit(test_knn_latency, q_vector) for _ in range(NUM_THREADS * 2)]
        for f in futures: f.result()
        
        # Основной тест
        t0 = time.time()
        futures = [executor.submit(test_knn_latency, q_vector) for _ in range(NUM_REQUESTS_THROUGHPUT)]
        for f in futures: f.result()
        elapsed = time.time() - t0
    return NUM_REQUESTS_THROUGHPUT / elapsed

def get_disk_usage():
    """Возвращает размер файла базы данных DuckDB."""
    duck_conn.execute('CHECKPOINT') # Сбрасываем данные на диск для точного измерения
    try:
        return os.path.getsize(DB_FILE)
    except FileNotFoundError:
        return 0

# --- Основной цикл тестирования ---
def main():
    results = []
    
    for n in SIZES:
        print(f"\n{'='*20} TESTING WITH N = {n:,} VECTORS {'='*20}")
        
        # 1. Генерация данных
        df = generate_vectors(n, DIM)
        query_vector = df['embedding'].iloc[np.random.randint(0, n)].tolist()
        
        # 2. Вставка
        print("Running Bulk Insert test...")
        bulk_insert_time = test_bulk_insert(df)
        print(f"  > Bulk Insert Time: {bulk_insert_time:.2f}s")
        
        # 3. Создание индекса
        print("Running Index Creation test...")
        index_creation_time = test_index_creation()
        print(f"  > Index Creation Time: {index_creation_time:.2f}s")

        # 4. Задержка
        print("Running Latency test...")
        latency = np.mean([test_knn_latency(query_vector) for _ in range(10)]) * 1000
        print(f"  > KNN Latency (avg of 10): {latency:.2f}ms")
        
        # 5. Пропускная способность
        print("Running Throughput test...")
        qps = test_knn_throughput(query_vector)
        print(f"  > Throughput: {qps:.0f} QPS")
        
        # 6. Использование диска
        print("Measuring disk usage...")
        disk_usage = get_disk_usage()
        print(f"  > Disk Usage: {disk_usage/1e6:.2f}MB")
        
        results.append({
            'size': n,
            'bulk_insert': bulk_insert_time,
            'index_creation': index_creation_time,
            'latency': latency,
            'qps': qps,
            'disk_usage': disk_usage,
        })

    df_res = pd.DataFrame(results)
    print("\n--- Final Results ---")
    print(df_res)
    
    # --- Построение и сохранение графиков ---
    print("\nGenerating and saving plots...")
    
    def plot_metric(x, y, y_label, title, file_name):
        plt.style.use('seaborn-v0_8-whitegrid')
        plt.figure(figsize=(10, 6))
        plt.plot(x, y, 'o-', label='DuckDB', color='dodgerblue')
        plt.xlabel('Number of Vectors')
        plt.ylabel(y_label)
        plt.title(title)
        plt.legend()
        # Форматируем ось X для лучшей читаемости (100000 -> 0.1M)
        plt.gca().get_xaxis().set_major_formatter(
            plt.matplotlib.ticker.FuncFormatter(lambda val, p: f'{val/1e6:.1f}M'))
        plt.savefig(f"{file_name}.png", dpi=150, bbox_inches='tight')
        plt.close()
        print(f" - Saved {file_name}.png")

    plot_metric(df_res['size'], df_res['bulk_insert'], 'Time (seconds)', 'DuckDB: Bulk Insert Performance', 'duckdb_bulk_insert')
    plot_metric(df_res['size'], df_res['index_creation'], 'Time (seconds)', 'DuckDB: Index Creation Time', 'duckdb_index_creation')
    plot_metric(df_res['size'], df_res['latency'], 'Latency (ms)', 'DuckDB: KNN Search Latency (k=10)', 'duckdb_knn_latency')
    plot_metric(df_res['size'], df_res['qps'], 'Queries Per Second (QPS)', f'DuckDB: KNN Throughput ({NUM_THREADS} threads)', 'duckdb_knn_throughput')
    plot_metric(df_res['size'], df_res['disk_usage']/1e6, 'Disk Usage (MB)', 'DuckDB: On-Disk Size (Data + Index)', 'duckdb_disk_usage')

    print("\nTests finished. All plots saved.")

if __name__ == '__main__':
    main()
