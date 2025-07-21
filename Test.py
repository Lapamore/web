def test_index_creation_ch():
    """
    Тестирует создание векторного индекса в ClickHouse.
    Используем современный синтаксис TYPE hnsw(...) для версии 24.x
    """
    ch_client.command('ALTER TABLE vectors_ch DROP INDEX IF EXISTS idx_vector_l2')
    
    t0 = time.time()
    
    # ### ИЗМЕНЕНИЕ: Используем прямой синтаксис TYPE hnsw(...) ###
    ch_client.command(
        f'''
        ALTER TABLE vectors_ch
        ADD INDEX idx_vector_l2 embedding TYPE hnsw(
            'type=L2Distance',          -- Тип метрики расстояния
            'm={HNSW_M}',               -- Максимальное число соседей на слое
            'ef_construction=200'       -- Параметр для построения индекса
        ) GRANULARITY 1
        ''',
        # Настройка для разрешения экспериментальной фичи все еще нужна
        settings={'allow_experimental_vector_similarity_index': 1}
    )
    
    # Индекс строится в фоне, но для теста нужно дождаться его готовности
    while True:
        status = ch_client.query('''
            SELECT status FROM system.vector_indices
            WHERE table = 'vectors_ch' AND name = 'idx_vector_l2'
        ''').result_rows
        if not status: # Индекс еще не появился в системной таблице
            time.sleep(1)
            continue
        if status[0][0] == 'Built':
            break
        elif status[0][0] == 'Error':
             # Добавим вывод ошибки, если индекс не построился
            error_msg = ch_client.query(
                "SELECT error FROM system.vector_indices WHERE table = 'vectors_ch' AND name = 'idx_vector_l2'"
            ).result_rows[0][0]
            raise Exception(f"ClickHouse vector index build failed: {error_msg}")
            
        print("Waiting for ClickHouse index to build...")
        time.sleep(2)
        
    return time.time() - t0
