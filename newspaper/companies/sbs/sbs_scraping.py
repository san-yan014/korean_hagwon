from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import json
import requests

# collect all article urls for a keyword+year with pagination
def collect_articles_for_keyword(driver, year, keyword):
    all_urls = {}  # {news_id: url}
    page = 1
    
    while True:
        search_url = f'https://news.sbs.co.kr/news/search/result.do?query={keyword}&collection=&startDate={year}-01-01&endDate={year}-12-31&searchOption=on&pageIdx={page}'
        
        print(f"    page {page}...")
        driver.get(search_url)
        time.sleep(2)
        
        # find article links
        links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="endPage.do?news_id="]')
        
        if not links:
            print(f"    no more results")
            break
        
        # extract unique news_ids
        page_count = 0
        for link in links:
            url = link.get_attribute('href')
            if url and 'news_id=' in url:
                news_id = url.split('news_id=')[1].split('&')[0]
                clean_url = f'https://news.sbs.co.kr/news/endPage.do?news_id={news_id}'
                if news_id not in all_urls:
                    all_urls[news_id] = clean_url
                    page_count += 1
        
        print(f"    found {page_count} new articles")
        
        if page_count == 0:
            break
        
        page += 1
    
    return all_urls

# scrape article content
def scrape_article(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # extract title
        title = soup.find('h1', class_='title') or soup.find('div', class_='title')
        
        # extract date - look in multiple places
        date = None
        # try article info area
        date_elem = soup.find('div', class_='date_area') or soup.find('span', class_='date_area')
        if date_elem:
            date = date_elem.get_text(strip=True)
        
        # try meta tags
        if not date:
            meta_date = soup.find('meta', property='article:published_time')
            if meta_date:
                date = meta_date.get('content', '')
        
        # try other common date locations
        if not date:
            date_elem = soup.find('div', class_='article_info') or soup.find('p', class_='date')
            if date_elem:
                date = date_elem.get_text(strip=True)
        
        # extract content
        content = soup.find('div', class_='text_area') or soup.find('div', id='cnbc_body')
        
        return {
            'url': url,
            'publication': 'SBS',
            'title': title.get_text(strip=True) if title else '',
            'date': date if date else '',
            'content': content.get_text(strip=True) if content else ''
        }
    except Exception as e:
        print(f"  error scraping {url}: {e}")
        return None

# main workflow
def main():
    # keywords to search
    keywords = [
        '학원 강사',
        '학원 교사',
        '학원 선생',
        '사교육 강사',
        '사교육 교사'
    ]
    
    # setup selenium
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # process each year
        for year in range(2005, 2020):
            print(f"\n{'='*60}")
            print(f"PROCESSING YEAR {year}")
            print(f"{'='*60}")
            
            # collect urls for each keyword, tracking duplicates
            all_articles = {}  # {news_id: {'url': url, 'keywords': [keyword1, keyword2]}}
            
            for keyword in keywords:
                print(f"\nsearching for '{keyword}' in {year}...")
                keyword_urls = collect_articles_for_keyword(driver, year, keyword)
                
                # add to master list, tracking which keywords found each article
                for news_id, url in keyword_urls.items():
                    if news_id not in all_articles:
                        all_articles[news_id] = {
                            'url': url,
                            'keywords': [keyword]
                        }
                    else:
                        all_articles[news_id]['keywords'].append(keyword)
                
                print(f"  total: {len(keyword_urls)} articles for '{keyword}'")
            
            print(f"\ntotal unique articles for {year}: {len(all_articles)}")
            
            # save url collection results
            with open(f'sbs_urls_{year}.json', 'w', encoding='utf-8') as f:
                json.dump(all_articles, f, ensure_ascii=False, indent=2)
            
            # scrape articles for this year
            print(f"\nscraping articles for {year}...")
            results = []
            article_list = list(all_articles.items())
            
            for i, (news_id, info) in enumerate(article_list):
                url = info['url']
                print(f"  {i+1}/{len(article_list)}: {url}")
                
                data = scrape_article(url)
                
                if data:
                    data['news_id'] = news_id
                    data['keywords_matched'] = info['keywords']
                    results.append(data)
                
                time.sleep(1)
                
                # save checkpoint every 50 articles
                if (i + 1) % 50 == 0:
                    with open(f'sbs_{year}_checkpoint_{i+1}.json', 'w', encoding='utf-8') as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
            
            # save final results for this year
            with open(f'sbs_{year}_articles.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            print(f"completed {year}: {len(results)} articles scraped")
        
    finally:
        driver.quit()
    
    print("\n" + "="*60)
    print("ALL YEARS COMPLETED")
    print("="*60)

if __name__ == '__main__':
    main()