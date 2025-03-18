import os
import pdfplumber
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple
import spacy
from keybert import KeyBERT
import nltk
from nltk.tokenize import sent_tokenize
nltk.download('punkt')

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
        self.nlp = spacy.load("en_core_web_sm")
        self.kw_model = KeyBERT()
        
    def extract_paragraphs_from_pdf(self, pdf_path: str) -> List[Dict]:
        """
        Extract paragraphs from a PDF file
        
        Args:
            pdf_path (str): Path to PDF file
            
        Returns:
            List[Dict]: List of dictionaries containing paragraph info
        """
        paragraphs = []
        
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                text = page.extract_text()
                if text:
                    # Split into paragraphs (assuming double newline as separator)
                    page_paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
                    
                    for para_num, paragraph in enumerate(page_paragraphs, 1):
                        paragraphs.append({
                            'document_name': os.path.basename(pdf_path),
                            'page_number': page_num,
                            'paragraph_number': para_num,
                            'text': paragraph,
                            'word_count': len(paragraph.split())
                        })
        
        return paragraphs

    def extract_key_phrases(self, text: str, top_n: int = 5) -> List[Tuple[str, float]]:
        """
        Extract key phrases from text using KeyBERT
        
        Args:
            text (str): Input text
            top_n (int): Number of key phrases to extract
            
        Returns:
            List[Tuple[str, float]]: List of (phrase, score) tuples
        """
        keywords = self.kw_model.extract_keywords(
            text,
            keyphrase_ngram_range=(1, 3),
            stop_words='english',
            top_n=top_n
        )
        return keywords

    def analyze_cost_drivers(self, text: str) -> List[str]:
        """
        Identify potential cost drivers in text using spaCy
        
        Args:
            text (str): Input text
            
        Returns:
            List[str]: List of identified cost-related terms
        """
        doc = self.nlp(text)
        
        # Look for money amounts, percentages, and cost-related terms
        cost_indicators = []
        
        # Money and percentage patterns
        for ent in doc.ents:
            if ent.label_ in ['MONEY', 'PERCENT']:
                cost_indicators.append(ent.text)
        
        # Cost-related terms
        cost_terms = ['cost', 'price', 'fee', 'charge', 'payment', 'expense', 'investment']
        for token in doc:
            if token.lemma_.lower() in cost_terms:
                # Get the full noun phrase if available
                if token.head.pos_ == 'NOUN':
                    phrase = ' '.join([t.text for t in token.head.subtree])
                else:
                    phrase = token.text
                cost_indicators.append(phrase)
        
        return list(set(cost_indicators))

    def process_all_pdfs(self):
        """
        Process all PDFs in the input directory and save results to Excel
        """
        all_paragraphs = []
        
        # Process each PDF file
        for filename in os.listdir(self.pdf_dir):
            if filename.endswith('.pdf'):
                pdf_path = os.path.join(self.pdf_dir, filename)
                paragraphs = self.extract_paragraphs_from_pdf(pdf_path)
                
                # Analyze each paragraph
                for para in paragraphs:
                    # Extract key phrases
                    key_phrases = self.extract_key_phrases(para['text'])
                    para['key_phrases'] = '; '.join([f"{phrase} ({score:.2f})" for phrase, score in key_phrases])
                    
                    # Identify cost drivers
                    cost_drivers = self.analyze_cost_drivers(para['text'])
                    para['cost_drivers'] = '; '.join(cost_drivers)
                
                all_paragraphs.extend(paragraphs)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_paragraphs)
        
        # Save to Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        excel_path = os.path.join(self.output_dir, f'eudr_analysis_{timestamp}.xlsx')
        df.to_excel(excel_path, index=False)
        print(f"Analysis complete. Results saved to {excel_path}")

if __name__ == "__main__":
    # Define directories relative to project root
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    pdf_dir = os.path.join(project_root, 'data', 'pdfs')
    output_dir = os.path.join(project_root, 'data', 'output')
    
    # Create processor and run analysis
    processor = PDFProcessor(pdf_dir, output_dir)
    processor.process_all_pdfs() 