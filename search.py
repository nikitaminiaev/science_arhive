#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import sqlite3


def check_database_integrity(db_path):
    """
    Проверяет целостность базы данных
    """
    print(f"🔍 Проверка целостности базы данных {db_path}...")
    
    try:
        conn = sqlite3.connect(db_path, timeout=30.0)
        cursor = conn.cursor()
        
        # Проверка целостности
        print("   Выполняется PRAGMA integrity_check...")
        cursor.execute("PRAGMA integrity_check")
        integrity_result = cursor.fetchone()[0]
        
        if integrity_result != "ok":
            print(f"❌ Проблема с целостностью БД: {integrity_result}")
            return False
        
        print("✅ Целостность БД в порядке")
        
        # Проверка размера и статистики
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        
        db_size_bytes = page_count * page_size
        db_size_mb = db_size_bytes / (1024 * 1024)
        
        print(f"📊 Размер БД: {db_size_mb:.1f} МБ ({page_count} страниц по {page_size} байт)")
        
        conn.close()
        return True
        
    except sqlite3.DatabaseError as e:
        print(f"❌ Ошибка целостности БД: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка при проверке БД: {e}")
        return False


def optimize_database_connection(db_path):
    """
    Создает оптимизированное подключение к большой БД (только для чтения)
    """
    try:
        # Параметры для работы с большими БД
        conn = sqlite3.connect(
            db_path,
            timeout=60.0,  # Увеличенный таймаут
            check_same_thread=False
        )
        
        # Оптимизация для больших БД (режим чтения)
        conn.execute("PRAGMA cache_size = -2097152")   # 2 ГБ кэш (2*1024*1024 КБ)
        conn.execute("PRAGMA temp_store = MEMORY")      # Временные данные в RAM
        conn.execute("PRAGMA synchronous = OFF")        # Отключаем синхронизацию для чтения
        conn.execute("PRAGMA journal_mode = MEMORY")    # Журнал в памяти для чтения
        conn.execute("PRAGMA mmap_size = 4294967296")   # 4 ГБ memory-mapped I/O
        conn.execute("PRAGMA read_uncommitted = true")  # Грязное чтение для скорости
        
        return conn
        
    except Exception as e:
        print(f"❌ Ошибка создания оптимизированного подключения: {e}")
        return None


def repair_database(db_path):
    """
    Попытка восстановления поврежденной БД
    """
    print(f"🔧 Попытка восстановления базы данных...")
    
    backup_path = db_path + ".backup"
    
    try:
        # Создаем резервную копию
        print(f"   Создание резервной копии: {backup_path}")
        
        source_conn = sqlite3.connect(db_path)
        backup_conn = sqlite3.connect(backup_path)
        
        # Экспортируем данные в новую БД
        source_conn.backup(backup_conn)
        
        source_conn.close()
        backup_conn.close()
        
        print("✅ Резервная копия создана успешно")
        print(f"💡 Попробуйте использовать файл: {backup_path}")
        
        return backup_path
        
    except Exception as e:
        print(f"❌ Ошибка восстановления БД: {e}")
        return None


def search_database():
    """
    Улучшенный интерфейс для поиска в базе данных PDF архива
    """
    print("=== ПОИСК ПО АРХИВУ PDF ДОКУМЕНТОВ (ОПТИМИЗИРОВАНО ДЛЯ БОЛЬШИХ БД) ===\n")
    
    # Запрашиваем путь к базе данных
    db_path = input("Введите путь к базе данных (или Enter для archive.db): ").strip()
    if not db_path:
        db_path = 'archive.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена!")
        return
    
    # Проверяем размер файла
    file_size = os.path.getsize(db_path)
    file_size_mb = file_size / (1024 * 1024)
    print(f"📊 Размер файла БД: {file_size_mb:.1f} МБ")
    
    # if file_size_mb > 100:
    #     print("⚠️  Обнаружена большая база данных. Выполняется диагностика...")
        
    #     # Проверяем целостность
    #     if not check_database_integrity(db_path):
    #         print("\n🔧 База данных повреждена. Попытка восстановления...")
    #         repaired_path = repair_database(db_path)
    #         if repaired_path:
    #             use_backup = input(f"Использовать восстановленную БД {repaired_path}? (y/n): ").strip().lower()
    #             if use_backup == 'y':
    #                 db_path = repaired_path
    #             else:
    #                 print("❌ Работа с поврежденной БД невозможна")
    #                 return
    #         else:
    #             print("❌ Восстановление не удалось")
    #             return
    
    try:
        # Создаем оптимизированное подключение
        print("🔗 Подключение к базе данных с оптимизацией...")
        conn = optimize_database_connection(db_path)
        
        if not conn:
            print("❌ Не удалось создать подключение к БД")
            return
            
        cursor = conn.cursor()
        
        # Проверяем существование таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pdf_metadata'")
        if not cursor.fetchone():
            print("❌ Таблица pdf_metadata не найдена! Возможно, база данных создана в старом формате.")
            return
        
        # Показываем статистику с оптимизацией для больших БД
        print("📊 Загрузка статистики...")
        
        cursor.execute("SELECT COUNT(*) FROM pdf_metadata")
        total_docs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM pdf_search_index")
        indexed_docs = cursor.fetchone()[0]
        
        print(f"📊 Статистика базы данных:")
        print(f"   Всего документов: {total_docs:,}")
        print(f"   Проиндексировано: {indexed_docs:,}")
        
        if total_docs == 0:
            print("\n❌ База данных пуста!")
            return
        
        print(f"\n🔍 Введите поисковый запрос (или 'exit' для выхода):")
        print("Примеры запросов:")
        print("  - machine learning")
        print("  - \"neural networks\"")
        print("  - (deep OR machine) AND learning")
        print("  - comput* AND vision*")
        print("\n💡 Справка по релевантности:")
        print("  ⭐⭐⭐ Очень высокая (< -10.0) - точные совпадения")
        print("  ⭐⭐ Высокая (-10.0 до -8.0) - хорошие совпадения") 
        print("  ⭐ Средняя (-8.0 до -5.0) - частичные совпадения")
        print("  ○ Низкая (> -5.0) - слабые совпадения")
        
        while True:
            query = input("\n> ").strip()
            
            if query.lower() in ['exit', 'quit', 'выход']:
                break
            
            if not query:
                continue
            
            try:
                # Оптимизированный поиск для больших БД
                print("🔍 Выполняется поиск...")
                
                cursor.execute('''
                    SELECT 
                        pm.id,
                        pm.zip_path,
                        pm.pdf_filename,
                        pm.metadata,
                        bm25(pdf_search_index) as bm25_score,
                        snippet(pdf_search_index, 0, '<b>', '</b>', '...', 100) as snippet
                    FROM pdf_metadata pm
                    JOIN pdf_search_index ON pm.id = pdf_search_index.rowid
                    WHERE pdf_search_index MATCH ?
                    ORDER BY bm25(pdf_search_index)
                    LIMIT 20
                ''', (query,))
                
                results = cursor.fetchall()
                
                if not results:
                    print("❌ Ничего не найдено.")
                    continue
                
                # Статистика по релевантности
                very_high = sum(1 for r in results if r[4] < -10.0)
                high = sum(1 for r in results if -10.0 <= r[4] < -8.0)
                medium = sum(1 for r in results if -8.0 <= r[4] < -5.0)
                low = sum(1 for r in results if r[4] >= -5.0)
                
                print(f"\n✅ Найдено результатов: {len(results)}")
                print(f"📈 Релевантность: ⭐⭐⭐{very_high} ⭐⭐{high} ⭐{medium} ○{low}")
                print("=" * 80)
                
                # Показываем результаты порциями для больших БД
                results_per_page = 5
                total_pages = (len(results) + results_per_page - 1) // results_per_page
                current_page = 1
                
                def show_results_page(page_num):
                    start_idx = (page_num - 1) * results_per_page
                    end_idx = min(start_idx + results_per_page, len(results))
                    
                    for i in range(start_idx, end_idx):
                        result = results[i]
                        doc_id, zip_path, pdf_filename, metadata, bm25_score, snippet = result
                        
                        # Парсим метаданные
                        try:
                            metadata_dict = json.loads(metadata) if metadata else {}
                        except (json.JSONDecodeError, TypeError):
                            metadata_dict = {}
                        
                        file_size = metadata_dict.get('file_size')
                        pages_count = metadata_dict.get('pages_count')
                        
                        print(f"\n{i+1}. 📄 {pdf_filename}")
                        print(f"   📁 Архив: {os.path.basename(zip_path)}")
                        if file_size:
                            print(f"   📏 Размер: {file_size:,} байт")
                        if pages_count:
                            print(f"   📃 Страниц: {pages_count}")
                        
                        # Интерпретация BM25 скора
                        if bm25_score < -10.0:
                            relevance_text = "⭐⭐⭐ Очень высокая"
                        elif bm25_score < -8.0:
                            relevance_text = "⭐⭐ Высокая"
                        elif bm25_score < -5.0:
                            relevance_text = "⭐ Средняя"
                        else:
                            relevance_text = "○ Низкая"
                        
                        print(f"   ⭐ Релевантность: {bm25_score:.4f} ({relevance_text})")
                        print(f"   📝 Фрагмент: {snippet}")
                        print("-" * 60)
                
                # Показываем первую страницу
                show_results_page(current_page)
                
                # Навигация по страницам для больших результатов
                while True:
                    if total_pages > 1:
                        print(f"\nСтраница {current_page} из {total_pages}")
                        nav_choice = input("Команды: [n]ext, [p]rev, [номер], [d]etail номер, [q]uit: ").strip().lower()
                    else:
                        nav_choice = input("Команды: [d]etail номер, [q]uit: ").strip().lower()
                    
                    if nav_choice == 'q' or nav_choice == 'quit':
                        break
                    elif nav_choice == 'n' and current_page < total_pages:
                        current_page += 1
                        show_results_page(current_page)
                    elif nav_choice == 'p' and current_page > 1:
                        current_page -= 1
                        show_results_page(current_page)
                    elif nav_choice.isdigit():
                        page_num = int(nav_choice)
                        if 1 <= page_num <= total_pages:
                            current_page = page_num
                            show_results_page(current_page)
                        else:
                            print(f"❌ Неверный номер страницы. Доступно: 1-{total_pages}")
                    elif nav_choice.startswith('d') and len(nav_choice.split()) > 1:
                        try:
                            doc_num = int(nav_choice.split()[1])
                            if 1 <= doc_num <= len(results):
                                result = results[doc_num - 1]
                                print(f"\n📍 Подробная информация о документе #{doc_num}:")
                                print(f"   Архив: {result[1]}")
                                print(f"   PDF: {result[2]}")
                                print(f"   ID в БД: {result[0]}")
                            else:
                                print(f"❌ Неверный номер документа. Доступно: 1-{len(results)}")
                        except ValueError:
                            print("❌ Неверный формат команды. Используйте: d номер")
                    else:
                        print("❌ Неверная команда")
            
            except sqlite3.OperationalError as e:
                if "database disk image is malformed" in str(e):
                    print("❌ База данных повреждена во время поиска!")
                    print("💡 Рекомендуется выполнить восстановление БД")
                    break
                else:
                    print(f"❌ Ошибка поиска: {e}")
            except Exception as e:
                print(f"❌ Неожиданная ошибка поиска: {e}")
    
    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
        if "database disk image is malformed" in str(e):
            print("\n💡 Рекомендации для решения проблемы:")
            print("1. База данных повреждена - используйте функцию восстановления")
            print("2. Проверьте свободное место на диске")
            print("3. Убедитесь, что БД не используется другими процессами")
            print("4. Возможно потребуется пересоздание индекса")
    
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("🔗 Соединение с БД закрыто")


if __name__ == "__main__":
    search_database() 