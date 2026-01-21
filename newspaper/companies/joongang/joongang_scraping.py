import requests
from bs4 import BeautifulSoup
import time
import json
import csv
from datetime import datetime
from urllib.parse import quote
import os

def search_joongang(keyword, start_date=None, end_date=None, page=1):
    """
    search joongang ilbo for articles containing keyword
    
    args:
        keyword: korean search term (e.g., "학원")
        start_date: start date in format "YYYY-MM-DD" (e.g., "2005-01-01")
        end_date: end date in format "YYYY-MM-DD" (e.g., "2019-12-31")
        page: page number for pagination
    
    returns:
        list of article urls found on this page
    """
    # correct joongang search url: /search/news?keyword=학원강사&startDate=2005-01-01&endDate=2019-12-31&sfield=all&page=2
    base_url = "https://www.joongang.co.kr/search/news"
    
    params = {
        'keyword': keyword,
        'sfield': 'all',  # search all fields
        'page': page
    }
    
    # add date filters if provided
    if start_date:
        params['startDate'] = start_date
    if end_date:
        params['endDate'] = end_date
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # extract article urls from search results
        # pattern: <a href="/article/..."> or <a href="https://www.joongang.co.kr/article/...">
        urls = []
        
        # find all links to articles
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # match article url pattern
            if '/article/' in href:
                # make full url if relative
                if href.startswith('/'):
                    full_url = f"https://www.joongang.co.kr{href}"
                else:
                    full_url = href
                
                # remove duplicates and query parameters
                clean_url = full_url.split('?')[0]
                if clean_url not in urls:
                    urls.append(clean_url)
        
        print(f"  found {len(urls)} article urls on page {page}")
        return urls
    
    except requests.exceptions.RequestException as e:
        print(f"  ✗ request error: {e}")
        return []
    except Exception as e:
        print(f"  ✗ error searching: {e}")
        return []


def search_joongang_all_pages(keyword, start_date=None, end_date=None, max_pages=50):
    """
    search multiple pages for a keyword
    
    args:
        keyword: search term
        start_date: start date "YYYY.MM.DD"
        end_date: end date "YYYY.MM.DD"
        max_pages: maximum pages to scrape
    
    returns:
        list of all article urls found
    """
    all_urls = []
    
    print(f"\nsearching joongang for '{keyword}'")
    if start_date and end_date:
        print(f"  date range: {start_date} to {end_date}")
    print("-" * 60)
    
    for page in range(1, max_pages + 1):
        print(f"[page {page}/{max_pages}]")
        
        urls = search_joongang(keyword, start_date, end_date, page)
        
        if not urls:
            print(f"  no results on page {page}, stopping search")
            break
        
        # remove duplicates before adding
        new_urls = [url for url in urls if url not in all_urls]
        all_urls.extend(new_urls)
        
        print(f"  cumulative total: {len(all_urls)} unique urls")
        
        # rate limiting
        time.sleep(2)
    
    print(f"\ntotal urls found for '{keyword}': {len(all_urls)}")
    return all_urls


def scrape_joongang_article(url):
    """
    scrape a single joongang article
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # extract metadata
        title = None
        date = None
        author = None
        publication = "JoongAng"
        category = None
        text = None
        
        # title from meta tag
        title_meta = soup.find('meta', {'property': 'og:title'})
        if title_meta:
            title = title_meta.get('content', '').strip()
            title = title.replace(' | 중앙일보', '').replace(' | JoongAng Ilbo', '')
        
        # date from meta tag
        date_meta = soup.find('meta', {'property': 'article:published_time'})
        if date_meta:
            date = date_meta.get('content', '').strip()
        
        # author from meta tag
        author_meta = soup.find('meta', {'name': 'author'})
        if author_meta:
            author = author_meta.get('content', '').strip()
        
        # category from meta tags
        section_meta = soup.find('meta', {'property': 'article:section2'})
        if section_meta:
            category = section_meta.get('content', '').strip()
        
        # extract article body
        article_body = soup.find('div', {'class': 'article_body', 'id': 'article_body'})
        
        if article_body:
            paragraphs = article_body.find_all('p', {'data-divno': True})
            
            if paragraphs:
                text = '\n'.join([p.get_text(strip=True) for p in paragraphs])
            else:
                all_paragraphs = article_body.find_all('p')
                text = '\n'.join([p.get_text(strip=True) for p in all_paragraphs])
        
        # fallback methods
        if not title:
            title_h1 = soup.find('h1', {'class': 'headline'})
            if title_h1:
                title = title_h1.get_text(strip=True)
        
        if not author:
            byline_div = soup.find('div', {'class': 'byline'})
            if byline_div:
                byline_link = byline_div.find('a')
                if byline_link:
                    author = byline_link.get_text(strip=True)
                    author = author.replace('기자', '').strip()
        
        if not date:
            time_tag = soup.find('time', {'itemprop': 'datePublished'})
            if time_tag:
                date = time_tag.get('datetime', time_tag.get_text(strip=True))
        
        if not category:
            url_parts = url.split('/')
            if len(url_parts) > 3:
                category = url_parts[3]
        
        return {
            'newspaper': 'JoongAng',
            'url': url,
            'title': title,
            'date': date,
            'author': author,
            'publication': publication,
            'category': category,
            'text': text,
            'scraped_at': datetime.now().isoformat()
        }
    
    except requests.exceptions.RequestException as e:
        print(f"request error for {url}: {e}")
        return None
    except Exception as e:
        print(f"error scraping {url}: {e}")
        return None


def scrape_articles_batch(urls, delay=1.0, save_interval=100):
    """
    scrape multiple articles with periodic saving
    """
    articles = []
    error_count = 0
    
    print(f"\nstarting batch scraping of {len(urls)} articles")
    print("=" * 60)
    
    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] scraping: {url[:60]}...")
        
        article = scrape_joongang_article(url)
        
        if article and article.get('text'):
            articles.append(article)
            print(f"  ✓ saved (text: {len(article['text'])} chars)")
        else:
            error_count += 1
            print(f"  ✗ failed or no text")
        
        # periodic checkpoint
        if i % save_interval == 0:
            checkpoint_file = f"checkpoint_search_{len(articles)}_articles.json"
            save_to_json(articles, checkpoint_file)
            print(f"\n→ checkpoint saved: {len(articles)} articles")
        
        # status update
        if i % 50 == 0:
            print(f"\n--- progress: {i}/{len(urls)} ---")
            print(f"    saved: {len(articles)} | errors: {error_count}")
            print()
        
        time.sleep(delay)
    
    print("\n" + "=" * 60)
    print(f"scraping complete!")
    print(f"  total processed: {len(urls)}")
    print(f"  saved articles: {len(articles)}")
    print(f"  errors: {error_count}")
    print("=" * 60)
    
    return articles


def save_to_json(articles, filename):
    """save articles to json file"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        print(f"✓ saved {len(articles)} articles to {filename}")
    except Exception as e:
        print(f"✗ error saving json: {e}")


def save_to_csv(articles, filename):
    """save articles to csv file"""
    if not articles:
        print("no articles to save")
        return
    
    try:
        fieldnames = ['newspaper', 'url', 'title', 'date', 'author', 'publication', 
                      'category', 'text', 'scraped_at']
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for article in articles:
                writer.writerow(article)
        
        print(f"✓ saved {len(articles)} articles to {filename}")
    except Exception as e:
        print(f"✗ error saving csv: {e}")


def main():
    """
    main execution function - search-based scraping
    """
    print("=" * 80)
    print("joongang ilbo search-based scraper")
    print("boston university hagwon stigmatization research")
    print("=" * 80)
    
    # ===== configuration =====
    
    # search keywords - adjust based on your research needs
    keywords = [
        '학원',      # hagwon
        '사교육',    # private education
        '학원강사',  # hagwon instructor
        '학원교사',  # hagwon teacher
    ]
    
    # date range (2005-2019 per your research doc)
    # format: YYYY-MM-DD (not YYYY.MM.DD)
    start_date = "2005-01-01"
    end_date = "2019-12-31"
    
    # search parameters
    max_pages_per_keyword = 400  

    delay_between_requests = 1.0  # seconds between scraping articles
    save_checkpoint_every = 100   # save progress every n articles
    
    # output directory
    output_dir = "joongang_search_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # ===== step 1: search for urls =====
    print("\n step 1: searching for article urls by keyword")
    print("-" * 80)
    
    # use set for automatic deduplication
    all_urls_set = set()
    
    for keyword in keywords:
        print(f"\nsearching for keyword: '{keyword}'")
        urls = search_joongang_all_pages(
            keyword=keyword,
            start_date=start_date,
            end_date=end_date,
            max_pages=max_pages_per_keyword
        )
        
        # add to set (automatic deduplication)
        urls_before = len(all_urls_set)
        all_urls_set.update(urls)
        urls_after = len(all_urls_set)
        
        new_urls_count = urls_after - urls_before
        duplicate_count = len(urls) - new_urls_count
        
        print(f"→ found {len(urls)} urls for '{keyword}'")
        print(f"→ added {new_urls_count} new unique urls")
        print(f"→ skipped {duplicate_count} duplicates")
        print(f"→ total unique urls so far: {len(all_urls_set)}\n")
        
        # delay between keyword searches
        time.sleep(2)
    
    print("=" * 80)
    print(f"url collection complete!")
    print(f"  total unique article urls: {len(all_urls_set)}")
    print(f"  duplicates removed: {sum([len(search_joongang_all_pages(kw, start_date, end_date, 0)) for kw in keywords]) - len(all_urls_set)}")
    print("=" * 80)
    
    # convert set to sorted list for consistent ordering
    all_urls = sorted(list(all_urls_set))
    
    if not all_urls:
        print("✗ no urls found. check search function or date parameters.")
        return
    
    # save urls to file for reference
    urls_file = os.path.join(output_dir, f"joongang_search_urls.txt")
    with open(urls_file, 'w', encoding='utf-8') as f:
        for url in all_urls:
            f.write(url + '\n')
    print(f"\n✓ saved {len(all_urls)} urls to {urls_file}")
    
    # ===== step 2: scrape articles =====
    print("\n\n step 2: scraping articles")
    print("-" * 80)
    
    articles = scrape_articles_batch(
        urls=all_urls,
        delay=delay_between_requests,
        save_interval=save_checkpoint_every
    )
    
    # ===== step 3: save final results =====
    print("\n\n step 3: saving final results")
    print("-" * 80)
    
    if articles:
        # save as json
        json_file = os.path.join(output_dir, f"joongang_hagwon_2005_2019_search.json")
        save_to_json(articles, json_file)
        
        # save as csv
        csv_file = os.path.join(output_dir, f"joongang_hagwon_2005_2019_search.csv")
        save_to_csv(articles, csv_file)
        
        print("\n" + "=" * 80)
        print("✓ search-based scraping completed successfully!")
        print(f"  final dataset: {len(articles)} hagwon-related articles")
        print(f"  date range: {start_date} to {end_date}")
        print(f"  output directory: {output_dir}/")
        print("=" * 80)
    else:
        print("\n✗ no articles were saved. check scraping function.")


if __name__ == "__main__":
    main()