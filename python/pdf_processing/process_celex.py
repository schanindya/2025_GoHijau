"""
Process CELEX PDF document using PyPDF2.
This script focuses on extracting text from the CELEX regulation document.
"""

import os
import PyPDF2
import pandas as pd
from datetime import datetime
import re

class CELEXProcessor:
    def __init__(self, pdf_dir: str, output_dir: str):
        """
        Initialize the CELEX document processor
        
        Args:
            pdf_dir (str): Directory containing PDF files
            output_dir (str): Directory for output files
        """
        self.pdf_dir = pdf_dir
        self.output_dir = output_dir
        
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize the extracted text
        """
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Fix common PDF artifacts
        text = text.replace('|', 'I')
        text = text.replace('"', '"').replace('"', '"')  # Smart quotes
        text = text.replace(''', "'").replace(''', "'")  # Smart apostrophes
        
        # Remove page numbers and headers/footers
        text = re.sub(r'^\d+$', '', text, flags=re.MULTILINE)
        text = re.sub(r'^Page \d+( of \d+)?$', '', text, flags=re.MULTILINE)
        
        return text.strip()
        
    def is_complete_sentence(self, text: str) -> bool:
        """
        Check if the text is a complete sentence
        """
        text = text.strip()
        if not text:
            return False
        if not text[0].isupper():
            return False
        if not text.rstrip()[-1] in ['.', '!', '?']:
            return False
        if len(text.split()) < 3:  # Minimum word count for a sentence
            return False
        return True
        
    def is_article_header(self, text: str) -> bool:
        """
        Check if text is an article header, with specific patterns for CELEX documents
        """
        article_patterns = [
            r'^Article\s+\d+',
            r'^CHAPTER\s+[IVX]+',
            r'^SECTION\s+\d+',
            r'^\d+\.\s*[A-Z]',  # For numbered sections
            r'^[A-Z]+\s+\d+',   # For "SECTION 1" style headers
            r'^ANNEX\s+[IVX]+',
            r'^Whereas:',
            r'^Having regard to'
        ]
        return any(re.match(pattern, text.strip()) for pattern in article_patterns)
        
    def split_into_sentences(self, text: str) -> list:
        """
        Split text into sentences while preserving article structure
        """
        # Clean up the text first
        text = self.clean_text(text)
        
        # First check if this is an article/section header
        if self.is_article_header(text):
            return [text]
            
        # Split on sentence endings while preserving the punctuation
        # More comprehensive sentence splitting
        sentence_endings = r'(?<=[.!?])\s+(?=[A-Z])'
        sentences = re.split(sentence_endings, text)
        
        complete_sentences = []
        current_sentence = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if self.is_article_header(sentence):
                # Save any accumulated sentence
                if current_sentence:
                    combined = ' '.join(current_sentence)
                    if self.is_complete_sentence(combined):
                        complete_sentences.append(combined)
                    current_sentence = []
                complete_sentences.append(sentence)
            elif self.is_complete_sentence(sentence):
                complete_sentences.append(sentence)
            else:
                current_sentence.append(sentence)
                combined = ' '.join(current_sentence)
                if self.is_complete_sentence(combined):
                    complete_sentences.append(combined)
                    current_sentence = []
                    
        # Handle any remaining text
        if current_sentence:
            combined = ' '.join(current_sentence)
            if self.is_complete_sentence(combined):
                complete_sentences.append(combined)
        
        return complete_sentences

    def extract_header_number(self, text: str) -> str:
        """
        Extract just the number/identifier from article headers
        
        Args:
            text (str): The full header text
            
        Returns:
            str: Just the article/chapter/section identifier
        """
        # Common patterns for headers
        patterns = [
            (r'^Article\s+(\d+)', 'Article {}'),
            (r'^CHAPTER\s+([IVX]+)', 'Chapter {}'),
            (r'^SECTION\s+(\d+)', 'Section {}'),
            (r'^ANNEX\s+([IVX]+)', 'Annex {}'),
        ]
        
        text = text.strip()
        
        # Try each pattern
        for pattern, template in patterns:
            match = re.match(pattern, text)
            if match:
                return template.format(match.group(1))
                
        # Special cases
        if text.startswith('Whereas:'):
            return 'Whereas'
        if text.startswith('Having regard to'):
            return 'Having regard'
            
        # If no pattern matches, return None
        return None

    def process_celex_pdf(self, filename: str = "CELEX_32023R1115_EN_TXT (1).pdf"):
        """
        Process the CELEX PDF file using PyPDF2
        """
        pdf_path = os.path.join(self.pdf_dir, filename)
        paragraphs = []
        current_article = None
        
        print(f"Opening {filename}...")
        
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            total_pages = len(pdf_reader.pages)
            print(f"Total pages: {total_pages}")
            
            for page_num in range(total_pages):
                print(f"Processing page {page_num + 1}/{total_pages}")
                page = pdf_reader.pages[page_num]
                text = page.extract_text()
                
                if text:
                    print(f"Page {page_num + 1}: Extracted {len(text)} characters")
                    
                    sections = text.split('\n\n')
                    
                    for section in sections:
                        if not section.strip():
                            continue
                            
                        sentences = self.split_into_sentences(section)
                        current_paragraph = []
                        
                        for sentence in sentences:
                            # Check if this is an article header
                            if self.is_article_header(sentence):
                                # Save any accumulated paragraph
                                if current_paragraph:
                                    paragraph_text = ' '.join(current_paragraph)
                                    if len(paragraph_text.split()) > 5:
                                        paragraphs.append({
                                            'page_number': page_num + 1,
                                            'paragraph_number': len(paragraphs) + 1,
                                            'article': current_article,
                                            'text': paragraph_text,
                                            'word_count': len(paragraph_text.split())
                                        })
                                current_paragraph = []
                                # Extract just the article number/identifier
                                current_article = self.extract_header_number(sentence)
                                # Add article header as its own entry
                                paragraphs.append({
                                    'page_number': page_num + 1,
                                    'paragraph_number': len(paragraphs) + 1,
                                    'article': current_article,
                                    'text': sentence,
                                    'word_count': len(sentence.split())
                                })
                                continue
                            
                            current_paragraph.append(sentence)
                            
                            # Start new paragraph after 3-4 sentences
                            if len(current_paragraph) >= 3:
                                paragraph_text = ' '.join(current_paragraph)
                                if len(paragraph_text.split()) > 5:
                                    paragraphs.append({
                                        'page_number': page_num + 1,
                                        'paragraph_number': len(paragraphs) + 1,
                                        'article': current_article,
                                        'text': paragraph_text,
                                        'word_count': len(paragraph_text.split())
                                    })
                                current_paragraph = []
                        
                        # Handle remaining sentences in the current paragraph
                        if current_paragraph:
                            paragraph_text = ' '.join(current_paragraph)
                            if len(paragraph_text.split()) > 5:
                                paragraphs.append({
                                    'page_number': page_num + 1,
                                    'paragraph_number': len(paragraphs) + 1,
                                    'article': current_article,
                                    'text': paragraph_text,
                                    'word_count': len(paragraph_text.split())
                                })
                else:
                    print(f"Warning: No text extracted from page {page_num + 1}")
        
        return paragraphs

    def save_results(self, paragraphs: list):
        """
        Save the extracted paragraphs to Excel
        """
        if not paragraphs:
            print("No paragraphs to save!")
            return None
            
        df = pd.DataFrame(paragraphs)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_path = os.path.join(self.output_dir, f'celex_paragraphs_{timestamp}.xlsx')
        
        df.to_excel(excel_path, index=False)
        print(f"\nResults saved to {excel_path}")
        print(f"Total paragraphs: {len(paragraphs)}")
        
        # Print some statistics
        articles = df['article'].nunique()
        avg_words = df['word_count'].mean()
        print(f"Number of unique articles: {articles}")
        print(f"Average words per paragraph: {avg_words:.1f}")
        
        return excel_path

def main():
    # Define directories relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pdf_dir = os.path.join(project_root, 'data', 'pdfs')
    output_dir = os.path.join(project_root, 'data', 'output')
    
    # Create processor
    processor = CELEXProcessor(pdf_dir, output_dir)
    
    # Process CELEX document
    print("Starting CELEX document processing...")
    paragraphs = processor.process_celex_pdf()
    
    # Save results
    processor.save_results(paragraphs)

if __name__ == "__main__":
    main()