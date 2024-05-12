from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def extract_links(url):
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    links = [link.get('href') for link in soup.find_all('a')]
    return links

def extract_article_data(url):
    reqs = requests.get(url)
    soup = BeautifulSoup(reqs.text, 'html.parser')
    titles = [title.text for title in soup.find_all('h2')]
    descriptions = [desc.text for desc in soup.find_all('p')]
    return titles, descriptions

def preprocess_text(text):
    """
    Preprocesses the extracted text data.
    """
    # Example of text preprocessing:
    # 1. Convert text to lowercase
    preprocessed_text = text.lower()
    
    # 2. Remove special characters, punctuation, and digits
    preprocessed_text = re.sub(r'[^a-zA-Z\s]', '', preprocessed_text)

    # 3. Remove extra whitespace
    preprocessed_text = re.sub(r'\s+', ' ', preprocessed_text).strip()

    # Add more preprocessing steps as needed

    return preprocessed_text

def scrape_and_save_data(url, output_file):
    links = extract_links(url)
    titles, descriptions = extract_article_data(url)
    
    # Ensure lengths of titles and descriptions are the same
    min_length = min(len(titles), len(descriptions))
    titles = titles[:min_length]
    descriptions = descriptions[:min_length]
    
    preprocessed_titles = [preprocess_text(title) for title in titles]
    preprocessed_descriptions = [preprocess_text(desc) for desc in descriptions]

    # Create a DataFrame
    data = pd.DataFrame({'Title': preprocessed_titles, 'Description': preprocessed_descriptions})

    # Save the data to a CSV file
    data.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    dawn_url = 'https://www.dawn.com/'
    bbc_url = 'https://www.bbc.com/'

    output_file_dawn = 'dawn_articles.csv'
    output_file_bbc = 'bbc_articles.csv'

    # Scrape and save data from Dawn.com
    scrape_and_save_data(dawn_url, output_file_dawn)

    # Scrape and save data from BBC.com
    scrape_and_save_data(bbc_url, output_file_bbc)


