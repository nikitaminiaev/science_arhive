#!/usr/bin/env python3
"""
Скрипт для повторной обработки PDF файлов, которые вызвали ошибки при первоначальной индексации.
Читает файл с ошибками и пытается повторно обработать проблемные PDF файлы.
"""

import argparse
import os
import re
import json
from typing import List, Tuple
from src.db_manager import PDFArchiveDatabaseManager
from filling_db import extract_pdf_text_from_zip, clean_text_encoding


def parse_error_file(error_file_path: str) -> List[Tuple[str, str, str]]:
    """
    Парсит файл с ошибками и извлекает информацию о неудачных PDF файлах.
    
    Args:
        error_file_path: Путь к файлу с ошибками
        
    Returns:
        List of tuples: (zip_path, pdf_filename, error_message)
    """
    failed_files = []
    
    if not os.path.exists(error_file_path):
        print(f"❌ Файл с ошибками не найден: {error_file_path}")
        return failed_files
    
    try:
        with open(error_file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
            
        # Разбиваем файл на блоки ошибок (каждый блок разделяется двумя \n)
        error_blocks = content.strip().split('\n\n')
        
        for block in error_blocks:
            if not block.strip():
                continue
                
            lines = block.strip().split('\n')
            zip_path = None
            pdf_filename = None
            error_message = None
            
            for line in lines:
                if line.startswith('zip_path: '):
                    zip_path = line[10:].strip()  # Убираем "zip_path: "
                elif line.startswith('pdf_filename: '):
                    pdf_filename = line[14:].strip()  # Убираем "pdf_filename: "
                elif line.startswith('error: '):
                    error_message = line[7:].strip()  # Убираем "error: "
            
            if zip_path and pdf_filename and error_message:
                failed_files.append((zip_path, pdf_filename, error_message))
                
    except Exception as e:
        print(f"❌ Ошибка при чтении файла с ошибками: {e}")
        
    return failed_files


def retry_failed_pdf(zip_path: str, pdf_filename: str, db_manager: PDFArchiveDatabaseManager, root_directory: str) -> bool:
    """
    Пытается повторно обработать один PDF файл.
    
    Args:
        zip_path: Путь к ZIP архиву
        pdf_filename: Имя PDF файла
        db_manager: Менеджер базы данных
        root_directory: Корневая директория (для относительного пути)
        
    Returns:
        True если успешно обработан, False иначе
    """
    try:
        # Проверяем, существует ли файл
        if not os.path.exists(zip_path):
            print(f"   ❌ ZIP архив не найден: {zip_path}")
            return False
            
        # Извлекаем текст из PDF
        raw_text, file_size, pages_count = extract_pdf_text_from_zip(zip_path, pdf_filename)
        
        if raw_text is None:
            print(f"   ❌ Не удалось извлечь текст из {pdf_filename}")
            return False
            
        # Очищаем данные
        clean_raw_text = clean_text_encoding(str(raw_text)) if raw_text else raw_text
        clean_pdf_filename = clean_text_encoding(pdf_filename)
        
        # Получаем относительный путь
        relative_zip_path = os.path.relpath(zip_path, root_directory)
        clean_relative_zip_path = clean_text_encoding(relative_zip_path)
        
        # Создаем метаданные
        metadata = json.dumps({"root_directory": root_directory, "retry": True})
        
        # Проверяем, не существует ли уже такая запись
        if db_manager.pdf_exists(clean_relative_zip_path, clean_pdf_filename):
            print(f"   ⚠️  PDF уже существует в БД: {pdf_filename}")
            return True
            
        # Сохраняем в базу данных
        db_manager.insert_pdf_content(
            clean_relative_zip_path, 
            clean_pdf_filename, 
            clean_raw_text, 
            metadata, 
            file_size, 
            pages_count
        )
        
        print(f"   ✅ Успешно обработан: {pdf_filename}")
        return True
        
    except Exception as e:
        print(f"   ❌ Ошибка при повторной обработке {pdf_filename}: {e}")
        return False


def write_retry_errors(failed_retries: List[Tuple[str, str, str]], output_file: str):
    """
    Записывает информацию о файлах, которые не удалось обработать повторно.
    
    Args:
        failed_retries: Список неудачных попыток
        output_file: Файл для записи ошибок
    """
    try:
        with open(output_file, 'w', encoding='utf-8', errors='replace') as f:
            f.write(f"# Файлы, которые не удалось обработать повторно\n")
            f.write(f"# Всего файлов: {len(failed_retries)}\n\n")
            
            for zip_path, pdf_filename, error in failed_retries:
                f.write(f"zip_path: {zip_path}\n")
                f.write(f"pdf_filename: {pdf_filename}\n")
                f.write(f"error: {error}\n\n")
                
        print(f"📝 Записано {len(failed_retries)} неудачных попыток в {output_file}")
        
    except Exception as e:
        print(f"❌ Ошибка при записи файла с ошибками: {e}")


def main():
    parser = argparse.ArgumentParser(description='Повторная обработка PDF файлов с ошибками')
    parser.add_argument('error_file', help='Путь к файлу с ошибками (обычно pdf_error.txt)')
    parser.add_argument('--db', default='archive.db', help='Путь к базе данных (по умолчанию: archive.db)')
    parser.add_argument('--root-dir', help='Корневая директория архивов (если не указана, будет определена автоматически)')
    parser.add_argument('--output', default='retry_errors.txt', help='Файл для записи новых ошибок')
    parser.add_argument('--batch-size', type=int, default=10, help='Размер батча для вывода прогресса')
    
    args = parser.parse_args()
    
    # Проверяем существование файла с ошибками
    if not os.path.exists(args.error_file):
        print(f"❌ Файл с ошибками не найден: {args.error_file}")
        return
        
    # Инициализируем менеджер базы данных
    db_manager = PDFArchiveDatabaseManager(args.db)
    conn = db_manager.create_database_schema()
    
    if not conn:
        print("❌ Не удалось подключиться к базе данных")
        return
        
    try:
        # Парсим файл с ошибками
        print("🔍 Анализ файла с ошибками...")
        failed_files = parse_error_file(args.error_file)
        
        if not failed_files:
            print("❌ Не найдено файлов для повторной обработки")
            return
            
        print(f"📁 Найдено файлов для повторной обработки: {len(failed_files)}")
        
        # Определяем корневую директорию, если не указана
        root_directory = args.root_dir
        if not root_directory:
            # Пытаемся определить общий корень из путей
            paths = [zip_path for zip_path, _, _ in failed_files]
            if paths:
                root_directory = os.path.commonpath(paths)
                # Поднимаемся на уровень выше, если нужно
                while not any(os.path.exists(os.path.join(root_directory, 'libgen.scimag')) for _ in [1]):
                    parent = os.path.dirname(root_directory)
                    if parent == root_directory:  # Достигли корня файловой системы
                        break
                    root_directory = parent
                    
        print(f"📂 Корневая директория: {root_directory}")
        
        # Обрабатываем файлы
        successful_count = 0
        failed_retries = []
        
        for i, (zip_path, pdf_filename, original_error) in enumerate(failed_files, 1):
            print(f"📄 [{i}/{len(failed_files)}] Обработка: {pdf_filename}")
            
            success = retry_failed_pdf(zip_path, pdf_filename, db_manager, root_directory)
            
            if success:
                successful_count += 1
            else:
                failed_retries.append((zip_path, pdf_filename, original_error))
                
            # Показываем прогресс
            if i % args.batch_size == 0:
                print(f"   📊 Прогресс: {i}/{len(failed_files)}, Успешно: {successful_count}, Ошибок: {len(failed_retries)}")
        
        # Финальный отчет
        print(f"\n🎉 Обработка завершена!")
        print(f"✅ Успешно обработано: {successful_count}")
        print(f"❌ Не удалось обработать: {len(failed_retries)}")
        print(f"📈 Процент успеха: {successful_count / len(failed_files) * 100:.1f}%")
        
        # Записываем новые ошибки
        if failed_retries:
            write_retry_errors(failed_retries, args.output)
            
    finally:
        conn.close()


if __name__ == "__main__":
    main() 