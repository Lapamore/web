def test_knn_throughput(q_vector):
    """
    Измеряет QPS (queries per second) в многопоточном режиме.
    КАЖДЫЙ ПОТОК СОЗДАЕТ СВОЕ СОБСТВЕННОЕ ПОДКЛЮЧЕНИЕ К БАЗЕ.
    """

    def run_query_in_thread(query_vec):
        """Эта функция будет выполняться в каждом потоке."""
        # Создаем новое, независимое подключение
        thread_conn = duckdb.connect(DB_FILE, read_only=True)
        # Устанавливаем необходимые расширения и настройки для этого подключения
        thread_conn.execute("LOAD vss;")
        
        # Выполняем сам запрос
        thread_conn.execute(
            f"SELECT id FROM vectors ORDER BY list_distance(embedding, {query_vec}) LIMIT {K}"
        ).fetchall()
        
        # Закрываем соединение
        thread_conn.close()

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        # Прогрев
        futures = [executor.submit(run_query_in_thread, q_vector) for _ in range(NUM_THREADS)]
        for f in futures: f.result()
        
        # Основной тест
        t0 = time.time()
        futures = [executor.submit(run_query_in_thread, q_vector) for _ in range(NUM_REQUESTS_THROUGHPUT)]
        for f in futures: f.result()
        elapsed = time.time() - t0
        
    return NUM_REQUESTS_THROUGHPUT / elapsed
