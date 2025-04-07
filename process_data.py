import sqlite3
import string
import csv
import matplotlib.pyplot as plt
from collections import Counter
import nltk
from nltk.corpus import stopwords

# Download stopwords if not already present.
nltk.download('stopwords')

DB_NAME = "facts.db"

def join_cat_facts_and_metadata():
    """
    Performs a JOIN query between CatFacts and CatFactMetadata.
    
    Returns:
        list of tuples: Each tuple contains (fact, fact_length, insertion_time)
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    query = '''
        SELECT CatFacts.fact, CatFactMetadata.fact_length, CatFactMetadata.insertion_time
        FROM CatFacts
        JOIN CatFactMetadata ON CatFacts.id = CatFactMetadata.cat_fact_id
    '''
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    return results

def get_all_cat_facts():
    """
    Retrieves all cat fact texts from the database.
    
    Returns:
        list of str: List of cat fact texts.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT fact FROM CatFacts")
    results = [row[0] for row in cur.fetchall()]
    conn.close()
    return results

def get_all_dog_facts():
    """
    Retrieves all dog fact texts from the database.
    
    Returns:
        list of str: List of dog fact texts.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT fact FROM DogFacts")
    results = [row[0] for row in cur.fetchall()]
    conn.close()
    return results

def clean_and_tokenize(text):
    """
    Lowercases the text, removes punctuation, splits it into words,
    and removes stopwords using the NLTK English stopwords list.
    
    Args:
        text (str): The text to clean and tokenize.
    
    Returns:
        list of str: List of words with stopwords removed.
    """
    text = text.lower()
    translator = str.maketrans('', '', string.punctuation)
    text = text.translate(translator)
    words = text.split()
    
    stop_words = set(stopwords.words('english'))
    filtered_words = [word for word in words if word not in stop_words]
    return filtered_words

def calculate_word_frequencies(facts):
    """
    Calculates the frequency distribution of words in a list of texts.
    
    Args:
        facts (list of str): List of fact texts.
    
    Returns:
        Counter: A Counter object with word frequencies.
    """
    all_words = []
    for fact in facts:
        words = clean_and_tokenize(fact)
        all_words.extend(words)
    return Counter(all_words)

def write_word_frequency_csv(freq_cat, freq_dog, combined_freq, filename="word_frequency.csv"):
    """
    Writes word frequency data to a CSV file.
    
    The CSV contains columns: word, freq_cat, freq_dog, freq_combined.
    
    Args:
        freq_cat (Counter): Word frequencies for cat facts.
        freq_dog (Counter): Word frequencies for dog facts.
        combined_freq (Counter): Combined word frequencies.
        filename (str): Name of the output CSV file.
    """
    #TODO
    pass

def visualize_top_words(freq, title, filename):
    """
    Creates a bar chart for the top 20 most frequent words.
    
    Args:
        freq (Counter): Word frequency Counter.
        title (str): Title for the chart.
        filename (str): Name of the output image file.
    """
    #TODO
    pass

def calculate_average_fact_length(joined_data):
    """
    Calculates the average fact length from the joined data.
    
    Args:
        joined_data (list of tuples): Each tuple contains (fact, fact_length, insertion_time)
    
    Returns:
        float: Average fact length.
    """
    if not joined_data:
        return 0
    total_length = sum(item[1] for item in joined_data)
    return total_length / len(joined_data)

def main():
    """
    Main function to process the stored data:
      - Performs a JOIN on CatFacts and CatFactMetadata to calculate average fact length.
      - Retrieves cat and dog fact texts.
      - Computes word frequency distributions for cat facts, dog facts, and the combined set.
      - Writes the calculated frequencies to a CSV file.
      - Creates three visualizations (bar charts) for the top 20 words.
    """
    #TODO
    pass

if __name__ == '__main__':
    main()