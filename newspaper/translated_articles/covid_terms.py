import json
import sys
import re
from datetime import datetime

# covid-related terms (korean and english)
COVID_TERMS = [
    '코로나', '우한', '폐렴', '신종', 'corona', 'wuhan', 'covid',
    '전염병', '감염병', '바이러스', 'virus', 'pandemic', 'epidemic',
    '우한폐렴', '신종폐렴', '중국폐렴'
]

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_year(article):
    # extract year from date field
    date_field = article.get('date', '')
    if not date_field:
        return None
    try:
        # handle "2005-01-05 00:02:00" format
        return int(str(date_field)[:4])
    except:
        return None

def search_covid_terms(text):
    # search for covid terms in text, return matches
    if not text:
        return []
    text_lower = text.lower()
    found = []
    for term in COVID_TERMS:
        if term.lower() in text_lower:
            found.append(term)
    return found

def main():
    if len(sys.argv) < 2:
        print("usage: python search_covid_terms.py <file1.json> <file2.json> ...")
        sys.exit(1)
    
    json_files = sys.argv[1:]
    
    total_articles = 0
    articles_2019 = 0
    covid_matches = []
    
    for filepath in json_files:
        print(f"loading: {filepath}")
        data = load_json(filepath)
        
        # handle both list and dict formats
        if isinstance(data, dict):
            articles = list(data.values())
        else:
            articles = data
        
        total_articles += len(articles)
        
        for article in articles:
            year = get_year(article)
            if year != 2019:
                continue
            
            articles_2019 += 1
            text = article.get('translated_text', '')
            matches = search_covid_terms(text)
            
            if matches:
                covid_matches.append({
                    'url': article.get('url', 'unknown'),
                    'date': article.get('date', 'unknown'),
                    'terms_found': matches,
                    'snippet': text[:500] if text else ''
                })
    
    # report results
    print(f"\n{'='*50}")
    print(f"total articles: {total_articles}")
    print(f"articles in 2019: {articles_2019}")
    print(f"covid-related articles found: {len(covid_matches)}")
    print(f"{'='*50}\n")
    
    if covid_matches:
        for i, match in enumerate(covid_matches, 1):
            print(f"--- match {i} ---")
            print(f"url: {match['url']}")
            print(f"date: {match['date']}")
            print(f"terms found: {', '.join(match['terms_found'])}")
            print(f"snippet: {match['snippet'][:300]}...")
            print()
    else:
        print("no covid-related terms found in 2019 articles.")

if __name__ == "__main__":
    main()