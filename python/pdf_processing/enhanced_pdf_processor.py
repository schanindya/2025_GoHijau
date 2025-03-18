"""
Enhanced PDF Processor using both PDFPlumber and PyPDF2 for improved text extraction.
This module provides more robust text extraction and paragraph processing capabilities.
"""

import os
import pdfplumber
import PyPDF2
import pandas as pd
from datetime import datetime
import re
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedPDFProcessor:
    def __init__(self, pdf_dir: str, output_dir: str):
        """
        Initialize the PDF processor with input and output directories.
        
        Args:
            pdf_dir (str): Directory containing PDF files to process
            output_dir (str): Directory where output Excel files will be saved
        """
        self.pdf_dir = pdf_dir
        self.output_dir = output_dir
        self.batch_size = 1000
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
    def extract_text_with_pdfplumber(self, pdf_path: str) -> list:
        """
        Extract text using PDFPlumber
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            list: List of extracted text from each page
        """
        pages_text = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
            logger.info(f"PDFPlumber extracted {len(pages_text)} pages from {os.path.basename(pdf_path)}")
        except Exception as e:
            logger.error(f"Error extracting text with PDFPlumber: {str(e)}")
        return pages_text

    def extract_text_with_pypdf2(self, pdf_path: str) -> list:
        """
        Extract text using PyPDF2
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            list: List of extracted text from each page
        """
        pages_text = []
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
            logger.info(f"PyPDF2 extracted {len(pages_text)} pages from {os.path.basename(pdf_path)}")
        except Exception as e:
            logger.error(f"Error extracting text with PyPDF2: {str(e)}")
        return pages_text

    def clean_text(self, text: str) -> str:
        """
        Clean and normalize extracted text
        
        Args:
            text (str): Raw text to clean
            
        Returns:
            str: Cleaned text
        """
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Handle common OCR and formatting issues
        text = text.replace('|', 'I')
        text = re.sub(r'["""]', '"', text)
        text = re.sub(r'[''']', "'", text)
        
        # Handle bullet points and lists
        text = re.sub(r'[•●○■]\s*', '- ', text)
        
        # Handle common PDF artifacts
        text = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)  # Split joined words
        text = re.sub(r'(?<=[.])(?=[a-zA-Z])', ' ', text)  # Add space after periods
        
        # Remove page numbers and headers/footers
        text = re.sub(r'^\d+$', '', text, flags=re.MULTILINE)
        text = re.sub(r'^Page \d+( of \d+)?$', '', text, flags=re.MULTILINE)
        
        return text.strip()

    def is_complete_sentence(self, text: str) -> bool:
        """
        Check if the text forms a complete sentence
        
        Args:
            text (str): Text to check
            
        Returns:
            bool: True if text is a complete sentence
        """
        text = text.strip()
        if not text:
            return False
            
        # Check basic sentence structure
        if not text[0].isupper():
            return False
            
        # Allow for multiple sentence endings
        if not text.rstrip()[-1] in ['.', '!', '?', ':', ';']:
            return False
            
        # Check minimum word count (avoid fragments)
        if len(text.split()) < 3:
            return False
            
        return True

    def split_into_sentences(self, text: str) -> list:
        """
        Split text into sentences while preserving formatting
        
        Args:
            text (str): Text to split into sentences
            
        Returns:
            list: List of sentences
        """
        # Split on sentence endings while preserving the punctuation
        sentence_pattern = r'(?<=[.!?])\s+(?=[A-Z])'
        sentences = re.split(sentence_pattern, text)
        
        # Filter and clean sentences
        valid_sentences = []
        for sentence in sentences:
            sentence = sentence.strip()
            if self.is_complete_sentence(sentence):
                valid_sentences.append(sentence)
                
        return valid_sentences

    def is_article_header(self, text: str) -> bool:
        """
        Check if text is an article header
        
        Args:
            text (str): Text to check
            
        Returns:
            bool: True if text is an article header
        """
        article_patterns = [
            r'^Article\s+\d+',
            r'^Pasal\s+\d+',
            r'^\d+\.\s*[A-Z]',
            r'^[A-Z]+\s+\d+',
            r'^Section\s+\d+',
            r'^CHAPTER\s+[IVX]+',
            r'^BAB\s+[IVX]+',
            r'^\(\d+\)',
            r'^[A-Z][A-Za-z\s]+:',
        ]
        return any(re.match(pattern, text.strip()) for pattern in article_patterns)

    def extract_paragraphs_from_pdf(self, pdf_path: str) -> list:
        """
        Extract paragraphs from PDF using both PDFPlumber and PyPDF2
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            list: List of dictionaries containing paragraph information
        """
        paragraphs = []
        current_article = None
        
        # Try both extraction methods
        plumber_pages = self.extract_text_with_pdfplumber(pdf_path)
        pypdf2_pages = self.extract_text_with_pypdf2(pdf_path)
        
        # Compare and use the better extraction for each page
        for page_num, (plumber_text, pypdf2_text) in enumerate(zip(plumber_pages, pypdf2_pages), 1):
            # Use the longer text (usually means better extraction)
            text = plumber_text if len(plumber_text) > len(pypdf2_text) else pypdf2_text
            text = self.clean_text(text)
            
            logger.info(f"Processing page {page_num} - Characters extracted: {len(text)}")
            
            sections = text.split('\n\n')
            current_paragraph = []
            
            for section in sections:
                sentences = self.split_into_sentences(section)
                
                for sentence in sentences:
                    if self.is_article_header(sentence):
                        # Save any accumulated paragraph
                        if current_paragraph:
                            paragraph_text = ' '.join(current_paragraph)
                            if len(paragraph_text.split()) > 5:
                                paragraphs.append({
                                    'document_name': os.path.basename(pdf_path),
                                    'page_number': page_num,
                                    'paragraph_number': len(paragraphs) + 1,
                                    'article': current_article,
                                    'text': paragraph_text,
                                    'word_count': len(paragraph_text.split()),
                                    'extraction_method': 'PDFPlumber' if text == plumber_text else 'PyPDF2'
                                })
                        current_paragraph = []
                        current_article = sentence
                        continue
                    
                    current_paragraph.append(sentence)
                    
                    # Start new paragraph after 3-4 sentences
                    if len(current_paragraph) >= 3:
                        paragraph_text = ' '.join(current_paragraph)
                        if len(paragraph_text.split()) > 5:
                            paragraphs.append({
                                'document_name': os.path.basename(pdf_path),
                                'page_number': page_num,
                                'paragraph_number': len(paragraphs) + 1,
                                'article': current_article,
                                'text': paragraph_text,
                                'word_count': len(paragraph_text.split()),
                                'extraction_method': 'PDFPlumber' if text == plumber_text else 'PyPDF2'
                            })
                        current_paragraph = []
            
            # Handle remaining text in the current paragraph
            if current_paragraph:
                paragraph_text = ' '.join(current_paragraph)
                if len(paragraph_text.split()) > 5:
                    paragraphs.append({
                        'document_name': os.path.basename(pdf_path),
                        'page_number': page_num,
                        'paragraph_number': len(paragraphs) + 1,
                        'article': current_article,
                        'text': paragraph_text,
                        'word_count': len(paragraph_text.split()),
                        'extraction_method': 'PDFPlumber' if text == plumber_text else 'PyPDF2'
                    })
        
        return paragraphs

    def save_batch(self, paragraphs: list, batch_number: int) -> str:
        """
        Save a batch of paragraphs to an Excel file
        
        Args:
            paragraphs (list): List of paragraph dictionaries to save
            batch_number (int): Batch number for the filename
            
        Returns:
            str: Path to the saved Excel file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'eudr_paragraphs_batch_{batch_number}_{timestamp}.xlsx'
        output_path = os.path.join(self.output_dir, filename)
        
        df = pd.DataFrame(paragraphs)
        df.to_excel(output_path, index=False)
        logger.info(f"Saved batch {batch_number} with {len(paragraphs)} paragraphs to {filename}")
        
        return output_path

    def process_all_pdfs(self) -> list:
        """
        Process all PDFs in the input directory
        
        Returns:
            list: List of paths to all saved Excel files
        """
        all_paragraphs = []
        saved_files = []
        batch_number = 1
        
        # Process each PDF file
        for filename in os.listdir(self.pdf_dir):
            if filename.lower().endswith('.pdf'):
                pdf_path = os.path.join(self.pdf_dir, filename)
                logger.info(f"Processing {filename}")
                
                try:
                    paragraphs = self.extract_paragraphs_from_pdf(pdf_path)
                    all_paragraphs.extend(paragraphs)
                    
                    # Save progress every batch_size paragraphs
                    while len(all_paragraphs) >= self.batch_size:
                        batch = all_paragraphs[:self.batch_size]
                        saved_files.append(self.save_batch(batch, batch_number))
                        all_paragraphs = all_paragraphs[self.batch_size:]
                        batch_number += 1
                        
                except Exception as e:
                    logger.error(f"Error processing {filename}: {str(e)}")
        
        # Save any remaining paragraphs
        if all_paragraphs:
            saved_files.append(self.save_batch(all_paragraphs, batch_number))
        
        return saved_files

def main():
    """
    Main function to demonstrate usage of the EnhancedPDFProcessor
    """
    # Set up directories
    pdf_dir = "data/pdfs"
    output_dir = "data/output"
    
    # Initialize processor
    processor = EnhancedPDFProcessor(pdf_dir, output_dir)
    
    # Process all PDFs
    saved_files = processor.process_all_pdfs()
    
    # Log results
    logger.info(f"Processing complete. Files saved: {len(saved_files)}")
    for file in saved_files:
        logger.info(f"Output file: {file}")

if __name__ == "__main__":
    main() 