import time
import os
import numpy as np
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- Параметры тестирования ---
SIZES = [100_000, 500_000, 1_000_000] # Размеры датасетов
DIM = 256                            # Размерность векторов, можно менять
K_DEFAULT = 10                       # Количество соседей по умолчанию
K_VALUES_TEST = [1, 10, 100]         # Разные значения K для теста масштабируемости
NUM_CATEGORIES = 100                 # Количество категорий для теста с фильтрами
NUM_REQUESTS_THROUGHPUT = 500        # Уменьшим для ускорения теста
NUM_THREADS = 8                      # Количество параллельных потоков

# --- Настройка DuckDB ---
DB_FILE = 'duckdb_advanced_benchmark.db'
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
duck_conn = duckdb.connect(DB_FILE)
print("DuckDB connection successful.")
# Включаем экспериментальные фичи и устанавливаем расширение один раз
duck_conn.execute("INSTALL vss; LOAD vss;")
duck_conn.execute("SET hnsw_enable_experimental_persistence=true;")
print("DuckDB VSS extension loaded and persistence enabled.")


# --- Расширенная генерация данных ---
def generate_data(n, dim):
    """Генерирует DataFrame с ID, векторами и полями для фильтрации."""
    print(f"Generating {n:,} rows of data (dim={dim})...")
    ids = np.arange(n, dtype=np.uint32)
    embs = np.random.random((n, dim)).astype(np.float32)
    embs /= np.linalg.norm(embs, axis=1, keepdims=True)
    categories = np.random.randint(0, NUM_CATEGORIES, size=n, dtype=np.uint16)
    prices = np.random.uniform(10.0, 1000.0, size=n).astype(np.float32)
    is_active = np.random.choice([True, False], size=n, p=[0.9, 0.1])
    
    return pd.DataFrame({
        'id': ids,
        'embedding': embs.tolist(),
        'category_id': categories,
        'price': prices,
        'is_active': is_active
    })


# --- Функции для тестирования DuckDB ---

def test_bulk_insert(df):
    """Тестирует вставку данных с правильной, динамической схемой таблицы."""
    duck_conn.execute('DROP TABLE IF EXISTS vectors')
    t0 = time.time()
    # ИСПРАВЛЕНИЕ: Используем переменную DIM для создания таблицы
    duck_conn.execute(f"""
        CREATE TABLE vectors (
            id UINTEGER, 
            embedding FLOAT[{DIM}],
            category_id USMALLINT,
            price REAL,
            is_active BOOLEAN
        )
    """)
    duck_conn.register('tmp_df', df)
    duck_conn.execute('INSERT INTO vectors SELECT * FROM tmp_df')
    duck_conn.unregister('tmp_df')
    return time.time() - t0

def test_index_creation():
    """Тестирует создание HNSW индекса."""
    duck_conn.execute("DROP INDEX IF EXISTS idx_hnsw")
    t0 = time.time()
    duck_conn.execute("CREATE INDEX idx_hnsw ON vectors USING HNSW(embedding)")
    return time.time() - t0

def test_knn_latency(q_vector, k):
    """Тестирует задержку чистого KNN запроса."""
    t0 = time.time()
    duck_conn.execute(
        f"SELECT id FROM vectors ORDER BY list_distance(embedding, {q_vector}) LIMIT {k}"
    ).fetchall()
    return time.time() - t0

# --- НОВЫЕ ТЕСТЫ ---

def test_filtered_knn_latency(q_vector, k, category_to_filter):
    """Тест: векторный поиск с WHERE-условием (фильтром)."""
    t0 = time.time()
    duck_conn.execute(f"""
        SELECT id, price 
        FROM vectors 
        WHERE category_id = {category_to_filter} AND is_active = true
        ORDER BY list_distance(embedding, {q_vector}) 
        LIMIT {k}
    """).fetchall()
    return time.time() - t0

def test_aggregation_on_knn(q_vector, k):
    """Тест: агрегация (AVG) по результатам векторного поиска."""
    t0 = time.time()
    duck_conn.execute(f"""
        SELECT avg(price), count(*)
        FROM (
            SELECT price 
            FROM vectors 
            ORDER BY list_distance(embedding, {q_vector}) 
            LIMIT {k}
        ) as subquery
    """).fetchall()
    return time.time() - t0

# --- КОНЕЦ НОВЫХ ТЕСТОВ ---

def test_knn_throughput(q_vector, k):
    """Измеряет QPS в многопоточном режиме (потокобезопасная версия)."""
    def run_query_in_thread(query_vec):
        thread_conn = duckdb.connect(DB_FILE, read_only=True)
        thread_conn.execute("LOAD vss;")
        thread_conn.execute(
            f"SELECT id FROM vectors ORDER BY list_distance(embedding, {query_vec}) LIMIT {k}"
        ).fetchall()
        thread_conn.close()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(run_query_in_thread, q_vector) for _ in range(NUM_THREADS)]
        for f in futures: f.result()
        t0 = time.time()
        futures = [executor.submit(run_query_in_thread, q_vector) for _ in range(NUM_REQUESTS_THROUGHPUT)]
        for f in futures: f.result()
        elapsed = time.time() - t0
    return NUM_REQUESTS_THROUGHPUT / elapsed

def get_disk_usage():
    """Возвращает размер файла БД."""
    duck_conn.execute('CHECKPOINT')
    try: return os.path.getsize(DB_FILE)
    except FileNotFoundError: return 0

# --- Основной цикл тестирования ---
def main():
    results = []
    
    for n in SIZES:
        print(f"\n{'='*25} TESTING WITH N = {n:,} VECTORS {'='*25}")
        
        # 1. Генерация и вставка данных
        df = generate_data(n, DIM)
        # ИСПРАВЛЕНИЕ: Преобразуем numpy.ndarray в список для SQL запроса
        query_vector = df['embedding'].iloc[0].tolist()
        category_to_filter = df['category_id'].iloc[0]
        
        print("\n--- Phase 1: Data Loading ---")
        bulk_insert_time = test_bulk_insert(df)
        print(f"  > Bulk Insert Time: {bulk_insert_time:.2f}s")
        
        # 2. Тест без индекса (полное сканирование)
        print("\n--- Phase 2: Pre-Index Tests (Full Scan) ---")
        latency_no_index = np.mean([test_knn_latency(query_vector, K_DEFAULT) for _ in range(3)]) * 1000
        print(f"  > Full Scan KNN Latency: {latency_no_index:.2f}ms")
        
        # 3. Создание индекса
        print("\n--- Phase 3: Index Creation ---")
        index_creation_time = test_index_creation()
        print(f"  > HNSW Index Creation Time: {index_creation_time:.2f}s")
        
        # 4. Тесты с использованием индекса
        print("\n--- Phase 4: Post-Index Tests ---")
        latency_indexed = np.mean([test_knn_latency(query_vector, K_DEFAULT) for _ in range(10)]) * 1000
        print(f"  > Indexed KNN Latency: {latency_indexed:.2f}ms ({(latency_no_index / latency_indexed if latency_indexed > 0 else 0):.1f}x faster than full scan)")
        
        latency_filtered = np.mean([test_filtered_knn_latency(query_vector, K_DEFAULT, category_to_filter) for _ in range(10)]) * 1000
        print(f"  > Filtered KNN Latency: {latency_filtered:.2f}ms")
        
        latency_aggregation = np.mean([test_aggregation_on_knn(query_vector, K_DEFAULT) for _ in range(10)]) * 1000
        print(f"  > Aggregation on KNN Latency: {latency_aggregation:.2f}ms")

        # 5. Тест на масштабируемость по K
        print("\n--- Phase 5: Scalability vs. K (number of neighbors) ---")
        for k_val in K_VALUES_TEST:
            lat = np.mean([test_knn_latency(query_vector, k_val) for _ in range(5)]) * 1000
            print(f"  > K = {k_val:<3} | Latency = {lat:.2f}ms")

        # 6. Тест пропускной способности
        print("\n--- Phase 6: Throughput Test ---")
        qps = test_knn_throughput(query_vector, K_DEFAULT)
        print(f"  > Throughput (k={K_DEFAULT}, {NUM_THREADS} threads): {qps:.0f} QPS")
        
        # 7. Использование диска
        disk_usage = get_disk_usage()
        print(f"\n--- Phase 7: Final Stats ---")
        print(f"  > Disk Usage: {disk_usage/1e6:.2f}MB")
        
        results.append({
            'size': n,
            'bulk_insert': bulk_insert_time,
            'index_creation': index_creation_time,
            'latency_no_index': latency_no_index,
            'latency_indexed': latency_indexed,
            'latency_filtered': latency_filtered,
            'qps': qps,
            'disk_usage': disk_usage,
        })

    df_res = pd.DataFrame(results)
    print("\n\n" + "="*20 + " FINAL RESULTS SUMMARY " + "="*20)
    print(df_res)
    
    # --- Построение и сохранение графиков ---
    print("\nGenerating and saving plots...")
    
    def plot_metric(df, y_cols, y_label, title, file_name, use_log_scale=False):
        plt.style.use('seaborn-v0_8-whitegrid')
        plt.figure(figsize=(10, 6))
        for col in y_cols:
            plt.plot(df['size'], df[col], 'o-', label=col.replace('_', ' ').title())
        plt.xlabel('Number of Vectors')
        plt.ylabel(y_label)
        plt.title(title)
        plt.legend()
        plt.gca().get_xaxis().set_major_formatter(
            plt.matplotlib.ticker.FuncFormatter(lambda val, p: f'{val/1e6:.1f}M'))
        if use_log_scale:
            plt.yscale('log')
        plt.savefig(f"{file_name}.png", dpi=150, bbox_inches='tight')
        plt.close()
        print(f" - Saved {file_name}.png")

    plot_metric(df_res, ['bulk_insert', 'index_creation'], 'Time (seconds)', 'DuckDB: Data Loading and Indexing Time', '1_load_and_index_time')
    plot_metric(df_res, ['latency_no_index', 'latency_indexed', 'latency_filtered'], 'Latency (ms)', 'DuckDB: KNN Search Latency Comparison', '2_latency_comparison', use_log_scale=True)
    plot_metric(df_res, ['qps'], 'Queries Per Second (QPS)', f'DuckDB: KNN Throughput ({NUM_THREADS} threads)', '3_throughput')
    plot_metric(df_res, ['disk_usage'], 'Disk Usage (MB)', 'DuckDB: On-Disk Size (Data + Index)', '4_disk_usage')

    print("\nTests finished. All plots saved.")

if __name__ == '__main__':
    main()
