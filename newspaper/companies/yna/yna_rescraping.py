"""
script will rescrape articles from yna to retrieve the entire body of text instead of snippets surrounding a matching keyword. will use the url links retrieved from the yna search engine to scrape entire
articles. will be in the same original format as before. 
"""
import json
import requests
from bs4 import BeautifulSoup
import time
import os

# configuration
input_json = "yna_output/yna_merged.json"  
output_json = "yna_second_output/yna_rescraped_articles.json" 
checkpoint_file = "yna_second_output/scrape_checkpoint.json"

# load checkpoint
scraped_data = []
scraped_cids = set()
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, 'r', encoding='utf-8') as f:
        scraped_data = json.load(f)
        scraped_cids = set(item['cid'] for item in scraped_data)
    print(f"resuming: {len(scraped_data)} articles already scraped")

# load input data
with open(input_json, 'r', encoding='utf-8') as f:
    articles = json.load(f)

print(f"total articles to process: {len(articles)}")

# scrape each article
for idx, article in enumerate(articles, 1):
    cid = article['cid']
    
    if cid in scraped_cids:
        continue
    
    url = article['url']
    
    try:
        print(f"[{idx}/{len(articles)}] scraping: {cid}")
        
        # fetch page
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # extract full article text
        article_tag = soup.find('article')
        if article_tag:
            paragraphs = article_tag.find_all('p')
            full_text = '\n'.join(p.get_text(strip=True) for p in paragraphs)
            
            # create new article dict with full body
            updated_article = article.copy()
            updated_article['body'] = full_text
            
            # add to scraped data
            scraped_data.append(updated_article)
            
            # save checkpoint
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(scraped_data, f, ensure_ascii=False, indent=2)
            
            print(f"  success: {len(full_text)} characters")
        else:
            print(f"  warning: no article tag found")
            scraped_data.append(article)  # keep original
        
        time.sleep(1)  # be polite to server
        
    except Exception as e:
        print(f"  error: {e}")
        scraped_data.append(article)  # keep original on error
        continue

# save final output
with open(output_json, 'w', encoding='utf-8') as f:
    json.dump(scraped_data, f, ensure_ascii=False, indent=2)

print(f"scraping complete: {len(scraped_data)} articles saved to {output_json}")