import os
import pdfplumber
import pandas as pd
from datetime import datetime
import re

class PDFProcessor:
    def __init__(self, pdf_dir: str, output_dir: str):
        """
        Initialize the PDF processor
        
        Args:
            pdf_dir (str): Directory containing PDF files
            output_dir (str): Directory for output files
        """
        self.pdf_dir = pdf_dir
        self.output_dir = output_dir
        self.batch_size = 1000  # Save every 1000 paragraphs
        
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
        return True
        
    def is_article_header(self, text: str) -> bool:
        """
        Check if text is an article header (e.g., "Article 1", "Pasal 2")
        """
        article_patterns = [
            r'^Article\s+\d+',
            r'^Pasal\s+\d+',
            r'^\d+\.\s*[A-Z]',  # For numbered sections
            r'^[A-Z]+\s+\d+',   # For "SECTION 1" style headers
        ]
        return any(re.match(pattern, text.strip()) for pattern in article_patterns)
        
    def split_into_sentences(self, text: str) -> list:
        """
        Split text into sentences using regex, preserving article structure
        """
        # Clean up the text
        text = text.replace('\n', ' ')
        text = re.sub(r'\s+', ' ', text)
        
        # First check if this is an article/section
        if self.is_article_header(text):
            return [text]
            
        # Split on sentence endings while preserving the punctuation
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        
        complete_sentences = []
        current_sentence = []
        
        for sentence in sentences:
            sentence = sentence.strip()
            if self.is_article_header(sentence):
                # If we have accumulated sentences, save them first
                if current_sentence:
                    combined = ' '.join(current_sentence)
                    if self.is_complete_sentence(combined):
                        complete_sentences.append(combined)
                    current_sentence = []
                # Add the article header as its own unit
                complete_sentences.append(sentence)
            elif self.is_complete_sentence(sentence):
                complete_sentences.append(sentence)
            else:
                current_sentence.append(sentence)
                combined = ' '.join(current_sentence)
                if self.is_complete_sentence(combined):
                    complete_sentences.append(combined)
                    current_sentence = []
                    
        if current_sentence:
            combined = ' '.join(current_sentence)
            if self.is_complete_sentence(combined):
                complete_sentences.append(combined)
        
        return complete_sentences

    def extract_paragraphs_from_pdf(self, pdf_path: str):
        """
        Extract paragraphs from a PDF file ensuring complete sentences and proper article handling
        """
        paragraphs = []
        current_article = None
        
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                if text:
                    sections = text.split('\n\n')
                    
                    for section in sections:
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
                                            'document_name': os.path.basename(pdf_path),
                                            'page_number': page_num,
                                            'paragraph_number': len(paragraphs) + 1,
                                            'article': current_article,
                                            'text': paragraph_text,
                                            'word_count': len(paragraph_text.split())
                                        })
                                current_paragraph = []
                                current_article = sentence
                                # Add article header as its own entry
                                paragraphs.append({
                                    'document_name': os.path.basename(pdf_path),
                                    'page_number': page_num,
                                    'paragraph_number': len(paragraphs) + 1,
                                    'article': current_article,
                                    'text': sentence,
                                    'word_count': len(sentence.split())
                                })
                                continue
                            
                            current_paragraph.append(sentence)
                            
                            # Start new paragraph after 3-4 sentences or at logical breaks
                            if len(current_paragraph) >= 3 and not any(
                                sentence.lower().startswith(connector) 
                                for connector in ['however', 'therefore', 'thus', 'furthermore', 
                                               'moreover', 'in addition', 'consequently']
                            ):
                                paragraph_text = ' '.join(current_paragraph)
                                if len(paragraph_text.split()) > 5:
                                    paragraphs.append({
                                        'document_name': os.path.basename(pdf_path),
                                        'page_number': page_num,
                                        'paragraph_number': len(paragraphs) + 1,
                                        'article': current_article,
                                        'text': paragraph_text,
                                        'word_count': len(paragraph_text.split())
                                    })
                                current_paragraph = []
                        
                        # Handle remaining sentences
                        if current_paragraph:
                            paragraph_text = ' '.join(current_paragraph)
                            if len(paragraph_text.split()) > 5:
                                paragraphs.append({
                                    'document_name': os.path.basename(pdf_path),
                                    'page_number': page_num,
                                    'paragraph_number': len(paragraphs) + 1,
                                    'article': current_article,
                                    'text': paragraph_text,
                                    'word_count': len(paragraph_text.split())
                                })
        
        return paragraphs

    def save_batch(self, paragraphs, batch_number):
        """
        Save a batch of paragraphs to Excel
        """
        df = pd.DataFrame(paragraphs)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_path = os.path.join(
            self.output_dir, 
            f'eudr_paragraphs_batch_{batch_number}_{timestamp}.xlsx'
        )
        df.to_excel(excel_path, index=False)
        print(f"Saved batch {batch_number} with {len(paragraphs)} paragraphs to {excel_path}")

    def process_all_pdfs(self):
        """
        Process all PDFs in the input directory and save results to Excel with periodic saving
        """
        all_paragraphs = []
        current_batch = 1
        
        # Process each PDF file
        pdf_files = [f for f in os.listdir(self.pdf_dir) if f.endswith('.pdf')]
        
        for filename in pdf_files:
            pdf_path = os.path.join(self.pdf_dir, filename)
            print(f"Processing {filename}...")
            
            paragraphs = self.extract_paragraphs_from_pdf(pdf_path)
            all_paragraphs.extend(paragraphs)
            print(f"Found {len(paragraphs)} paragraphs in {filename}")
            
            # Save progress when we reach batch_size
            if len(all_paragraphs) >= self.batch_size * current_batch:
                # Calculate which paragraphs belong to this batch
                start_idx = (current_batch - 1) * self.batch_size
                end_idx = current_batch * self.batch_size
                batch_paragraphs = all_paragraphs[start_idx:end_idx]
                
                # Save the batch
                self.save_batch(batch_paragraphs, current_batch)
                current_batch += 1
        
        # Save any remaining paragraphs
        if len(all_paragraphs) > (current_batch - 1) * self.batch_size:
            start_idx = (current_batch - 1) * self.batch_size
            remaining_paragraphs = all_paragraphs[start_idx:]
            if remaining_paragraphs:
                self.save_batch(remaining_paragraphs, current_batch)
        
        # Save complete dataset
        df_complete = pd.DataFrame(all_paragraphs)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_path = os.path.join(self.output_dir, f'eudr_all_paragraphs_complete_{timestamp}.xlsx')
        df_complete.to_excel(excel_path, index=False)
        
        print(f"\nProcessing complete. Total paragraphs: {len(all_paragraphs)}")
        print(f"Final complete dataset saved to {excel_path}")

if __name__ == "__main__":
    # Define directories relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pdf_dir = os.path.join(project_root, 'data', 'pdfs')
    output_dir = os.path.join(project_root, 'data', 'output')
    
    # Create processor and run analysis for all PDFs
    processor = PDFProcessor(pdf_dir, output_dir)
    processor.process_all_pdfs() 