#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import sqlite3
from db_manager import PDFArchiveDatabaseManager


def search_database():
    """
    Простой интерфейс для поиска в базе данных PDF архива
    """
    print("=== ПОИСК ПО АРХИВУ PDF ДОКУМЕНТОВ ===\n")
    
    # Запрашиваем путь к базе данных
    db_path = input("Введите путь к базе данных (или Enter для archive.db): ").strip()
    if not db_path:
        db_path = 'archive.db'
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена!")
        return
    
    try:
        # Подключаемся к базе данных напрямую
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем существование таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pdf_metadata'")
        if not cursor.fetchone():
            print("❌ Таблица pdf_metadata не найдена! Возможно, база данных создана в старом формате.")
            return
        
        # Показываем статистику
        cursor.execute("SELECT COUNT(*) FROM pdf_metadata")
        total_docs = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM pdf_search_index")
        indexed_docs = cursor.fetchone()[0]
        
        print(f"📊 Статистика базы данных:")
        print(f"   Всего документов: {total_docs}")
        print(f"   Проиндексировано: {indexed_docs}")
        
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
                # Выполняем поиск
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
                    LIMIT 10
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
                
                for i, result in enumerate(results, 1):
                    doc_id, zip_path, pdf_filename, metadata, bm25_score, snippet = result
                    
                    # Парсим метаданные
                    try:
                        metadata_dict = json.loads(metadata) if metadata else {}
                    except (json.JSONDecodeError, TypeError):
                        metadata_dict = {}
                    
                    file_size = metadata_dict.get('file_size')
                    pages_count = metadata_dict.get('pages_count')
                    
                    print(f"\n{i}. 📄 {pdf_filename}")
                    print(f"   📁 Архив: {zip_path}")
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
                
                # Предлагаем показать полный путь
                choice = input("\nВведите номер документа для показа полного пути (или Enter): ").strip()
                if choice.isdigit():
                    idx = int(choice) - 1
                    if 0 <= idx < len(results):
                        result = results[idx]
                        print(f"\n📍 Полный путь к файлу:")
                        print(f"   Архив: {result[1]}")
                        print(f"   PDF: {result[2]}")
            
            except Exception as e:
                print(f"❌ Ошибка поиска: {e}")
    
    except Exception as e:
        print(f"❌ Ошибка подключения к базе данных: {e}")
    
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    search_database() 