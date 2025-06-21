# PDF Archive Database Manager

## Project Description

This project provides a Python-based solution for processing and storing PDF contents from ZIP archives into an optimized SQLite database. It enables efficient text extraction, indexing, and full-text search capabilities for large collections of PDF files with minimal storage overhead.

## Features

- Extract text from PDFs within ZIP archives
- Optimized storage: metadata separate from search index
- Advanced full-text search with BM25 ranking
- Porter stemming and prefix search support
- Batch processing of ZIP archives
- Resume interrupted processing
- Comprehensive search interface
- Robust error handling and logging

## Database Structure

The project uses an optimized two-table structure to minimize storage while maximizing search performance:

### `pdf_metadata` Table
| Column       | Type    | Description                                  |
|--------------|---------|----------------------------------------------|
| id           | INTEGER | Primary key, auto-incrementing               |
| zip_path     | TEXT    | Relative path to the source ZIP archive      |
| pdf_filename | TEXT    | Original filename of the PDF                 |
| metadata     | TEXT    | JSON-formatted metadata (file_size, pages_count, etc.) |
| created_at   | DATETIME| Timestamp of when the record was created     |

**Note:** This table does NOT store the raw text content to save space. File size and page count are stored in the JSON metadata field.

### `pdf_search_index` Table
A virtual FTS5 (Full-Text Search) table with advanced features:
- **BM25 ranking** for relevance scoring
- **Porter stemming** for English language support
- **Prefix search** support (e.g., `machine*`)
- **Unicode normalization** for international text
- **Snippet generation** for search result previews

The search index is linked to metadata via `rowid` mapping.

## Prerequisites

- Python 3.8+

## Установка на Windows

### Шаг 1: Установка Python

1. Скачайте Python с официального сайта: https://www.python.org/downloads/windows/
2. Выберите версию Python 3.8 или выше
3. При установке обязательно поставьте галочку "Add Python to PATH"
4. Проверьте установку, открыв командную строку (Win+R → cmd) и выполнив:
   ```cmd
   python --version
   pip --version
   ```

### Шаг 2: Подготовка проекта

1. Скачайте проект (например, как ZIP-архив с GitHub)
2. Распакуйте в удобную папку, например: `C:\pdf_archive`
3. Откройте командную строку от имени администратора
4. Перейдите в папку проекта:
   ```cmd
   cd C:\pdf_archive
   ```

### Шаг 3: Установка зависимостей

Выполните команду:
```cmd
pip install PyPDF2
```

### Шаг 4: Запуск

#### Создание базы данных:
```cmd
python filling_db.py
```

При запросе пути к ZIP-архивам введите полный путь, например:
```
C:\Users\YourName\Documents\PDFs
D:\Архивы\PDF_коллекция  
C:\Downloads\Books_and_Papers
```

При запросе пути к базе данных можно просто нажать Enter (сохранится в папке программы).

#### Поиск по базе данных:
```cmd
python search.py
```

### Возможные проблемы на Windows

#### Проблема с кодировкой
Если возникают ошибки с русскими символами, добавьте в начало командной строки:
```cmd
chcp 65001
```

#### Проблема с путями
Используйте прямые слеши или двойные обратные слеши в путях:
- Правильно: `C:/path/to/files` или `C:\\path\\to\\files`
- Неправильно: `C:\path\to\files`

#### Альтернативный способ запуска
Если команда `python` не работает, попробуйте:
```cmd
py filling_db.py
py search.py
```

### Создание bat-файлов для удобства

Создайте файл `run_indexing.bat`:
```batch
@echo off
chcp 65001 > nul
cd /d "%~dp0"
python filling_db.py
pause
```

Создайте файл `run_search.bat`:
```batch
@echo off
chcp 65001 > nul
cd /d "%~dp0"
python search.py
pause
```

Теперь можно запускать программы двойным кликом по bat-файлам.

## Installation

### Для Linux/Mac:
1. Clone the repository
2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Для Windows:
1. Скачайте проект и распакуйте в папку
2. Запустите `install_dependencies.bat` двойным кликом
3. Готово! Теперь можно использовать `run_indexing.bat` и `run_search.bat`

## Usage

### 1. Building the Database

Run the indexing script:

```bash
python filling_db.py
```

When prompted:
1. Enter the full path to the directory containing ZIP archives
2. Optionally specify a custom path for the SQLite database (press Enter for default)

### 2. Searching the Database

Run the interactive search interface:

```bash
python search.py
```

### Examples

#### Building Database:
```
Введите путь к директории с ZIP-архивами: /path/to/your/zip/archives
Введите путь к базе данных (нажмите Enter для использования значения по умолчанию): 
```

#### Search Queries:
```bash
# Simple search
machine learning

# Exact phrase
"neural networks"

# Boolean search
(deep OR machine) AND learning NOT biology

# Prefix search
neur* AND comput*

# Exclude terms
quantum NOT physics
```

## Key Functionality

### Indexing (`filling_db.py`)
- Recursively searches for ZIP files in the specified directory
- Extracts text from PDFs (configurable: up to 10 pages)
- Stores up to 10,000 characters per PDF in search index
- Captures file metadata (size, page count)
- Supports resuming interrupted processing
- Comprehensive error logging

### Searching (`search_test.py`)
- Interactive search interface with real-time results
- BM25 relevance scoring
- Search result snippets with highlighting
- Database statistics display
- Direct file path access for found documents

### Database Manager (`db_manager.py`)
- Optimized schema for minimal storage overhead
- Advanced FTS5 configuration with stemming
- Programmatic search API
- Database statistics and maintenance

## Architecture Benefits

✅ **Space Efficient:** Text stored only in search index, not duplicated  
✅ **Fast Search:** BM25 ranking with stemming and prefix support  
✅ **Resumable:** Can restart processing from last processed file  
✅ **Scalable:** Handles large archives with batch processing  
✅ **Flexible:** Supports complex boolean search queries  

## Error Handling

- Errors during ZIP or PDF processing are logged to `pdf_error.txt`
- The script can resume from the last successfully processed file
- Graceful handling of corrupted or password-protected files
- Database transaction rollback on batch insert failures

## Limitations

- Extracts only the first 10 pages of each PDF (configurable)
- Truncates extracted text to 10,000 characters (configurable)
- No OCR support for image-based PDFs
- Limited to ZIP archives containing PDF files

## Advanced Usage

### Programmatic API
```python
import json
from db_manager import PDFArchiveDatabaseManager

db = PDFArchiveDatabaseManager('archive.db')
results = db.search_documents('machine learning', limit=5)
for result in results:
    doc_id, zip_path, pdf_filename, metadata, bm25_score, snippet = result
    metadata_dict = json.loads(metadata) if metadata else {}
    file_size = metadata_dict.get('file_size', 'Unknown')
    pages_count = metadata_dict.get('pages_count', 'Unknown')
    print(f"File: {pdf_filename}, Score: {bm25_score:.4f}, Size: {file_size}, Pages: {pages_count}")
```

### Custom Search Queries
```python
# Boolean search
results = db.search_documents('(neural OR deep) AND network')

# Phrase search  
results = db.search_documents('"artificial intelligence"')

# Prefix search
results = db.search_documents('comput* AND vision*')
```

## TODO

- [ ] OCR support for image-based PDFs
- [ ] Document title extraction from PDF metadata
- [ ] Vector embeddings for semantic search
- [ ] Web interface for search
- [ ] Export search results to various formats