import os
import zipfile
import io
import PyPDF2
from transformers import pipeline


def extract_pdf_text_from_zip(zip_path, pdf_filename, max_chars=1000):
    """
    Extract text from the first page of a PDF within a ZIP archive
    Focuses on capturing key information efficiently

    Args:
    - zip_path: Path to the ZIP archive
    - pdf_filename: Name of the PDF file within the archive
    - max_chars: Maximum number of characters to extract (default 1000)

    Returns:
    - Extracted text from the first page, truncated to max_chars
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            with zip_ref.open(pdf_filename) as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)

                # Check if the PDF has at least one page
                if len(pdf_reader.pages) > 0:
                    # Extract text from the first page
                    first_page_text = pdf_reader.pages[0].extract_text()

                    # Clean and truncate the text
                    cleaned_text = ' '.join(first_page_text.split())  # Remove extra whitespace
                    truncated_text = cleaned_text[:max_chars]

                    return truncated_text
                else:
                    return "Empty PDF"

    except Exception as e:
        return f"Error extracting text: {str(e)}"


def summarize_text(text, max_length=150, min_length=50):
    """Generate a concise summary of the text"""
    summarizer = pipeline("summarization")
    summaries = summarizer(text, max_length=max_length, min_length=min_length, do_sample=False)
    return summaries[0]['summary_text']


def process_zip_archives(root_directory):
    """Recursively process ZIP archives and create an index"""
    pdf_index = []

    for root, dirs, files in os.walk(root_directory):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)

                try:
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        pdf_files = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]

                        for pdf_filename in pdf_files:
                            try:
                                # Extract text from PDF without full archive extraction
                                pdf_text = extract_pdf_text_from_zip(zip_path, pdf_filename)

                                # Generate summary
                                summary = summarize_text(pdf_text)

                                # Create index entry
                                pdf_index.append({
                                    'zip_path': zip_path,
                                    'pdf_filename': pdf_filename,
                                    'summary': summary
                                })

                            except Exception as pdf_error:
                                print(f"Error processing PDF {pdf_filename} in {zip_path}: {pdf_error}")

                except Exception as zip_error:
                    print(f"Error processing ZIP archive {zip_path}: {zip_error}")

    return pdf_index


def write_to_file(error, zip_path, pdf_filename='', output_file='pdf_index.txt'):
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write(f"zip_path: {zip_path}\n")
        f.write(f"pdf_filename: {pdf_filename}\n")
        f.write(f"error: {error}\n\n")


# Example usage
root_directory = '/path/to/your/zip/archives'
pdf_index = process_zip_archives(root_directory)
