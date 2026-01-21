import requests
import ssl
import json
import time
import os
from datetime import datetime
from urllib3.util.ssl_ import create_urllib3_context

class YonhapAdapter(requests.adapters.HTTPAdapter):
    """adapter for yonhap's legacy ssl"""
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

def create_session():
    """create session with ssl adapter"""
    session = requests.Session()
    session.mount('https://ars.yna.co.kr', YonhapAdapter())
    return session

def search_yonhap(session, query, from_date, to_date, page=1, page_size=100):
    """search yonhap api"""
    
    url = "https://ars.yna.co.kr/api/v2/search.basic"
    
    params = {
        'query': query,
        'page_no': page,
        'page_size': page_size,
        'scope': 'all',
        'sort': 'date',
        'channel': 'basic_kr',
        'div_code': 'all',
        'from': from_date,
        'to': to_date,
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    }
    
    resp = session.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()

def load_checkpoint(checkpoint_file):
    """load checkpoint if exists"""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'completed_keywords': [], 'articles': {}}

def save_checkpoint(checkpoint_file, data):
    """save checkpoint"""
    with open(checkpoint_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def scrape_keyword(session, keyword, from_date, to_date):
    """scrape all articles for a keyword"""
    
    articles = []
    seen_cids = set()
    
    # get first page to find total
    data = search_yonhap(session, keyword, from_date, to_date, page=1)
    
    yib = data.get('YIB_KR_A', {})
    total = yib.get('totalCount', 0)
    
    print(f"  total articles: {total}")
    
    if total == 0:
        return articles
    
    total_pages = (total // 100) + 1
    
    for page in range(1, total_pages + 1):
        try:
            if page > 1:
                data = search_yonhap(session, keyword, from_date, to_date, page=page)
            
            results = data.get('YIB_KR_A', {}).get('result', [])
            
            for article in results:
                cid = article.get('CID')
                if cid and cid not in seen_cids:
                    seen_cids.add(cid)
                    articles.append({
                        'cid': cid,
                        'title': article.get('TITLE', '').replace('<b>', '').replace('</b>', ''),
                        'body': article.get('BODY', '').replace('<b>', '').replace('</b>', ''),
                        'datetime': article.get('DATETIME'),
                        'publication': "Yonhap",
                        'url': f"https://www.yna.co.kr/view/{cid}",
                        'keyword': article.get('KEYWORD'),
                        'writer': article.get('WRITER_NAME'),
                        'search_keyword': keyword,
                    })
            
            print(f"  page {page}/{total_pages}: {len(results)} articles (total unique: {len(articles)})")
            time.sleep(0.3)
            
        except Exception as e:
            print(f"  error on page {page}: {e}")
            time.sleep(1)
            continue
    
    return articles

def main():
    # config
    keywords = [
        '학원',       # hagwon
        '사교육',     # private education
        '학원강사',   # hagwon instructor (no space)
        '학원 강사',  # hagwon instructor (with space)
        '학원교사',   # hagwon teacher (no space)
        '학원 교사',  # hagwon teacher (with space)
        '강사',       # instructor alone (catches 강사 where 학원 mentioned elsewhere)
    ]
    
    from_date = '20050101'
    to_date = '20191231'
    
    output_dir = 'output_2'
    checkpoint_file = 'yonhap_checkpoint.json'
    
    os.makedirs(output_dir, exist_ok=True)
    
    # load checkpoint
    checkpoint = load_checkpoint(checkpoint_file)
    completed = set(checkpoint['completed_keywords'])
    all_articles = checkpoint['articles']
    
    print(f"yonhap scraper")
    print(f"date range: {from_date} - {to_date}")
    print(f"keywords: {keywords}")
    print(f"already completed: {list(completed)}")
    print("=" * 60)
    
    session = create_session()
    
    for keyword in keywords:
        if keyword in completed:
            print(f"\n[{keyword}] already completed, skipping")
            continue
        
        print(f"\n[{keyword}] starting...")
        
        try:
            articles = scrape_keyword(session, keyword, from_date, to_date)
            
            # save keyword results
            all_articles[keyword] = articles
            
            output_file = os.path.join(output_dir, f'yonhap_{keyword}_{from_date}_{to_date}.json')
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            
            print(f"  saved {len(articles)} articles to {output_file}")
            
            # update checkpoint
            completed.add(keyword)
            checkpoint['completed_keywords'] = list(completed)
            checkpoint['articles'] = all_articles
            save_checkpoint(checkpoint_file, checkpoint)
            
        except Exception as e:
            print(f"  error: {e}")
            continue
    
    # final summary
    print("\n" + "=" * 60)
    print("summary")
    print("=" * 60)
    
    total_articles = 0
    all_cids = set()
    
    for keyword, articles in all_articles.items():
        print(f"  {keyword}: {len(articles)} articles")
        total_articles += len(articles)
        for a in articles:
            all_cids.add(a['cid'])
    
    print(f"\ntotal articles: {total_articles}")
    print(f"unique articles: {len(all_cids)}")
    
    # save combined unique articles
    combined = []
    seen = set()
    for keyword in keywords:
        for article in all_articles.get(keyword, []):
            if article['cid'] not in seen:
                seen.add(article['cid'])
                combined.append(article)
    
    combined_file = os.path.join(output_dir, f'yonhap_combined_{from_date}_{to_date}.json')
    with open(combined_file, 'w', encoding='utf-8') as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)
    
    print(f"saved {len(combined)} unique articles to {combined_file}")

if __name__ == "__main__":
    main()