# PDF Archive Database Manager

## Project Description

This project provides a Python-based solution for processing and storing PDF contents from ZIP archives into a SQLite database. It enables efficient text extraction, indexing, and full-text search capabilities for large collections of PDF files.

## Features

- Extract text from PDFs within ZIP archives
- Store PDF contents in a SQLite database
- Support for full-text search
- Batch processing of ZIP archives
- Robust error handling and logging

## Database Structure

The project uses two main database tables:

### `pdf_contents` Table
| Column       | Type    | Description                                  |
|--------------|---------|----------------------------------------------|
| id           | INTEGER | Primary key, auto-incrementing               |
| zip_path     | TEXT    | Relative path to the source ZIP archive      |
| pdf_filename | TEXT    | Original filename of the PDF                 |
| raw_text     | TEXT    | Extracted text content from the PDF          |
| metadata     | TEXT    | JSON-formatted metadata about the processing |
| created_at   | DATETIME| Timestamp of when the record was created     |

### `pdf_contents_fts` Table
A virtual FTS5 (Full-Text Search) table that allows efficient text searching across extracted PDF contents.

## Prerequisites

- Python 3.8+

## Installation

1. Clone the repository
2. Install required dependencies:
   ```
   pip install sqlite3 PyPDF2
   ```

## Usage

Run the script from the command line:

```bash
python filling_db.py
```

When prompted:
1. Enter the full path to the directory containing ZIP archives
2. Optionally specify a custom path for the SQLite database (press Enter for default)

### Example

```
Введите путь к директории с ZIP-архивами: /path/to/your/zip/archives
Введите путь к базе данных (нажмите Enter для использования значения по умолчанию): 
```

## Key Functionality

- Recursively searches for ZIP files in the specified directory
- Extracts text from PDFs (limited to first 10 pages)
- Stores up to 10,000 characters per PDF
- Supports resuming interrupted processing
- Logs errors during processing

## Error Handling

- Errors during ZIP or PDF processing are logged to `pdf_error.txt`
- The script can resume from the last successfully processed file

## Limitations

- Extracts only the first 10 pages of each PDF
- Truncates extracted text to 10,000 characters
- Assumes all ZIP archives contain PDF files

## TODO

- [ ] Extract document titles from PDF metadata or first page
- [ ] Implement embedding generation for Retrieval-Augmented Generation (RAG)
  - Research and select appropriate embedding model
  - Create script for generating embeddings
  - Store embeddings alongside PDF contents