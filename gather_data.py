import requests
import sqlite3
import os
import datetime

DB_NAME = "facts.db"

def create_tables():
    """
    Creates the necessary tables in the SQLite database if they do not already exist.
    
    Tables:
      - CatFacts: stores cat fact text.
      - CatFactMetadata: stores metadata for each cat fact (fact_length and insertion_time).
      - DogFacts: stores dog fact text.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # Create table for cat facts with fact text as UNIQUE.
    cur.execute('''
        CREATE TABLE IF NOT EXISTS CatFacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fact TEXT UNIQUE
        )
    ''')
    
    # Create table for cat fact metadata.
    cur.execute('''
        CREATE TABLE IF NOT EXISTS CatFactMetadata (
            cat_fact_id INTEGER PRIMARY KEY,
            fact_length INTEGER,
            insertion_time TEXT,
            FOREIGN KEY (cat_fact_id) REFERENCES CatFacts(id)
        )
    ''')
    
    # Create table for dog facts.
    cur.execute('''
        CREATE TABLE IF NOT EXISTS DogFacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fact TEXT UNIQUE
        )
    ''')
    
    conn.commit()
    conn.close()

def fetch_cat_facts(n=10):
    """
    Fetches n new random cat facts using the /fact endpoint. 
    Each fact is checked to ensure it does not already exist in the database or in the current batch.
    If a duplicate is encountered, a replacement fact is fetched.
    
    Returns:
        list of dict: List of cat fact objects in the format {'fact': fact_text}
    """
    # Get existing cat facts from the database.
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT fact FROM CatFacts")
    existing_facts = set(row[0] for row in cur.fetchall())
    conn.close()
    
    new_facts = set()
    attempts = 0
    max_attempts = n * 20  # Safety cap in case of many duplicates.
    
    while len(new_facts) < n and attempts < max_attempts:
        try:
            response = requests.get("https://catfact.ninja/fact")
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code} for cat fact")
                attempts += 1
                continue
            
            data = response.json()
            fact_text = data.get("fact")
            if fact_text and fact_text not in existing_facts and fact_text not in new_facts:
                new_facts.add(fact_text)
            # If the fact is a duplicate, simply try again.
        except Exception as e:
            print("Error fetching cat fact:", e)
        attempts += 1
    
    if len(new_facts) < n:
        print(f"Warning: Only obtained {len(new_facts)} new cat facts after {attempts} attempts")
    
    return [{"fact": fact} for fact in new_facts]

def fetch_dog_facts(n=10):
    """
    Fetches n unique dog facts from the Dog API by kinduff in increments of 5 per request.
    Each fact is checked against existing facts in the database and within the current batch.
    If a duplicate is encountered, a replacement fact is fetched.
    
    Returns:
        list of str: List of unique dog fact strings.
    """
    # Get existing dog facts from the database.
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT fact FROM DogFacts")
    existing_facts = set(row[0] for row in cur.fetchall())
    conn.close()
    
    unique_facts = set()
    attempts = 0
    max_attempts = n * 20  # Safety cap in case of many duplicates.
    
    while len(unique_facts) < n and attempts < max_attempts:
        try:
            url = "https://dogapi.dog/api/v2/facts"
            params = {"limit": 5}
            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code} for dog facts")
                attempts += 1
                continue
            
            data = response.json()
            facts_list = data.get("data", [])
            for fact in facts_list:
                text = fact.get("attributes", {}).get("body")
                if text and text not in existing_facts and text not in unique_facts:
                    unique_facts.add(text)
            # If any facts are duplicates, they are simply ignored and we fetch another batch.
        except Exception as e:
            print("Error fetching dog facts:", e)
        attempts += 1
    
    if len(unique_facts) < n:
        print(f"Warning: Only obtained {len(unique_facts)} unique dog facts after {attempts} attempts")
    
    return list(unique_facts)[:n]

def store_cat_facts(facts):
    """
    Inserts cat facts into the CatFacts table and corresponding metadata into CatFactMetadata.
    
    Args:
        facts (list of dict): List of cat fact objects.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    for fact in facts:
        text = fact.get("fact")
        if text is None:
            continue
        try:
            cur.execute("INSERT INTO CatFacts (fact) VALUES (?)", (text,))
            new_id = cur.lastrowid
            # Calculate metadata.
            fact_length = len(text)
            insertion_time = datetime.datetime.now().isoformat()
            cur.execute(
                "INSERT INTO CatFactMetadata (cat_fact_id, fact_length, insertion_time) VALUES (?, ?, ?)", 
                (new_id, fact_length, insertion_time)
            )
            conn.commit()
        except sqlite3.IntegrityError:
            # Fact already exists; skip duplicate.
            continue
    conn.close()

def store_dog_facts(facts):
    """
    Inserts dog facts into the DogFacts table.
    
    Args:
        facts (list of str): List of dog fact strings.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    for fact in facts:
        if fact is None:
            continue
        try:
            cur.execute("INSERT INTO DogFacts (fact) VALUES (?)", (fact,))
            conn.commit()
        except sqlite3.IntegrityError:
            # Fact already exists; skip duplicate.
            continue
    conn.close()

def main():
    """
    Main function to create tables and fetch/store 10 new cat facts and 10 new dog facts.
    """
    create_tables()
    
    # Fetch and store 10 new cat facts using the /fact endpoint.
    cat_facts = fetch_cat_facts(n=10)
    store_cat_facts(cat_facts)
    
    # Fetch and store 10 unique dog facts.
    dog_facts = fetch_dog_facts(n=10)
    store_dog_facts(dog_facts)
    
    print("Data gathering complete. New facts (if any) have been stored in the database.")

if __name__ == '__main__':
    main()
