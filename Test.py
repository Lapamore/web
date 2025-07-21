def test_index_creation_ch():
    """
    Тестирует создание векторного индекса в ClickHouse.
    Финальная версия синтаксиса для этого сервера.
    """
    ch_client.command('ALTER TABLE vectors_ch DROP INDEX IF EXISTS idx_vector_l2')
    
    t0 = time.time()
    
    # ### ЭТО ПРАВИЛЬНЫЙ СИНТАКСИС ###
    ch_client.command(
        f'''
        ALTER TABLE vectors_ch
        ADD INDEX idx_vector_l2 embedding TYPE vector_similarity('L2Distance') GRANULARITY 1
        ''',
        # Эта настройка все еще нужна
        settings={'allow_experimental_vector_similarity_index': 1}
    )
    
    # Ожидание построения индекса
    while True:
        status = ch_client.query('''
            SELECT status FROM system.vector_indices
            WHERE table = 'vectors_ch' AND name = 'idx_vector_l2'
        ''').result_rows
        if not status:
            time.sleep(1)
            continue
        if status[0][0] == 'Built':
            print("ClickHouse index has been built successfully.")
            break
        elif status[0][0] == 'Error':
            error_msg = ch_client.query(
                "SELECT error FROM system.vector_indices WHERE table = 'vectors_ch' AND name = 'idx_vector_l2'"
            ).result_rows[0][0]
            raise Exception(f"ClickHouse vector index build failed: {error_msg}")
            
        print("Waiting for ClickHouse index to build...")
        time.sleep(2)
        
    return time.time() - t0
