import requests
from bs4 import BeautifulSoup
import time
import json
import csv
from datetime import datetime
import os
import xml.etree.ElementTree as ET
import gzip
import sys

def load_checkpoint_for_range(start_idx, end_idx, checkpoint_dir="."):
    """
    load checkpoint for this specific range if it exists
    """
    checkpoint_file = f"checkpoint_donga_{start_idx}_{end_idx}.json"
    checkpoint_path = os.path.join(checkpoint_dir, checkpoint_file)
    
    if not os.path.exists(checkpoint_path):
        print(f"  no existing checkpoint found for range {start_idx}-{end_idx}")
        return [], set()
    
    print(f"  found existing checkpoint: {checkpoint_file}")
    
    try:
        with open(checkpoint_path, 'r', encoding='utf-8') as f:
            articles = json.load(f)
            scraped_urls = set(article['url'] for article in articles)
        print(f"  loaded {len(articles)} articles, {len(scraped_urls)} urls")
        return articles, scraped_urls
    except Exception as e:
        print(f"  error loading checkpoint: {e}")
        return [], set()


def get_sitemap_urls(sitemap_url):
    """
    extract article urls from sitemap (handles sitemap index, regular sitemaps, and gzipped files)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(sitemap_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # handle gzipped content
        if sitemap_url.endswith('.gz'):
            content = gzip.decompress(response.content)
        else:
            content = response.content
        
        root = ET.fromstring(content)
        
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # check if this is a sitemap index
        sitemaps = root.findall('.//ns:sitemap', namespace)
        
        if sitemaps:
            # this is a sitemap index, recursively get urls from child sitemaps
            print(f"  found sitemap index with {len(sitemaps)} child sitemaps")
            all_urls = []
            
            for sitemap in sitemaps:
                loc = sitemap.find('ns:loc', namespace)
                if loc is not None and loc.text:
                    # filter for 2005-2019 date range
                    sitemap_name = loc.text
                    should_process = False
                    
                    # check if sitemap is in our date range
                    for year in range(2005, 2020):
                        if str(year) in sitemap_name:
                            should_process = True
                            break
                    
                    if should_process:
                        print(f"  reading child sitemap: {loc.text}")
                        child_urls = get_sitemap_urls(loc.text)
                        all_urls.extend(child_urls)
                        time.sleep(1)
                    else:
                        print(f"  skipping (outside date range): {loc.text}")
            
            return all_urls
        
        # this is a regular sitemap with urls
        urls = []
        for url in root.findall('.//ns:url', namespace):
            loc = url.find('ns:loc', namespace)
            if loc is not None and loc.text:
                urls.append(loc.text)
        
        print(f"  found {len(urls)} urls in sitemap")
        return urls
    
    except Exception as e:
        print(f"  ✗ error reading sitemap: {e}")
        return []


def scrape_donga_article(url):
    """
    scrape a single donga article
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # title
        title = None
        title_meta = soup.find('meta', {'property': 'og:title'})
        if title_meta:
            title = title_meta.get('content', '').strip()
        
        # date
        date = None
        date_meta = soup.find('meta', {'property': 'og:pubdate'})
        if date_meta:
            date = date_meta.get('content', '').strip()
        
        # category
        category = None
        category_meta = soup.find('meta', {'property': 'dd:category'})
        if category_meta:
            category = category_meta.get('content', '').strip()
        
        # article text - in <section class="news_view">
        text = None
        author = None
        news_view = soup.find('section', {'class': 'news_view'})
        
        if news_view:
            # remove script tags
            for script in news_view.find_all('script'):
                script.decompose()
            
            # get all text
            full_text = news_view.get_text(separator='\n', strip=True)
            
            # extract author from last line
            lines = full_text.split('\n')
            if lines:
                last_line = lines[-1]
                if 'reporter' in last_line.lower() or '기자' in last_line or '@donga.com' in last_line:
                    if 'Reporter' in last_line:
                        parts = last_line.split('Reporter')
                        if len(parts) > 1:
                            name_part = parts[1].split('@')[0].strip()
                            author = name_part
                    elif '기자' in last_line:
                        parts = last_line.split('기자')[0]
                        if '=' in parts:
                            author = parts.split('=')[-1].strip()
                        else:
                            author = parts.strip()
                    
                    text = '\n'.join(lines[:-1]).strip()
                else:
                    text = full_text
            else:
                text = full_text
        
        return {
            'newspaper': 'DongA',
            'url': url,
            'title': title,
            'date': date,
            'author': author,
            'publication': 'DongA',
            'category': category,
            'text': text,
            'scraped_at': datetime.now().isoformat()
        }
    
    except Exception as e:
        print(f"error scraping {url}: {e}")
        return None


def verify_keywords_double_filter(articles):
    """
    double filter: articles must have keywords AND hagwon instructor terms
    excludes false positives like 대학원 강사, 교육원 강사, etc.
    """
    keywords = ['학원', '학원강사', '학원교사', '사교육']
    
    # false positive patterns to exclude
    false_positive_patterns = [
        '대학원 강사', '대학원강사',
        '교육원 강사', '교육원강사',
        '연수원 강사', '연수원강사',
        '훈련원 강사', '훈련원강사',
        '문화원 강사', '문화원강사',
        '평생교육원 강사',
        '직업훈련원 강사'
    ]
    
    # non-academic hagwon keywords to exclude
    non_academic_keywords = [
        '미술학원', '미술 학원', '화실',
        '음악학원', '음악 학원', '피아노학원', '피아노 학원',
        '댄스학원', '댄스 학원', '발레', '방송댄스',
        '체육학원', '체육 학원', '태권도', '유도', '검도', '수영', '축구', '골프',
        '연기학원', '연기 학원', '뮤지컬'
    ]
    
    print("\n" + "=" * 80)
    print("keyword verification and double filtering")
    print("=" * 80)
    
    verified_articles = []
    rejected_articles = []
    keyword_counts = {kw: 0 for kw in keywords}
    combination_counts = {}
    instructor_types = {'학원강사': 0, '학원 강사': 0, '학원교사': 0, '학원 교사': 0}
    rejection_reasons = {
        'false_positive': 0,
        'non_academic': 0,
        'missing_keywords': 0,
        'missing_instructor_terms': 0
    }
    
    for i, article in enumerate(articles, 1):
        title = article.get('title', '') or ''
        text = article.get('text', '') or ''
        combined_text = title + ' ' + text
        
        # check for false positives first
        is_false_positive = any(pattern in combined_text for pattern in false_positive_patterns)
        if is_false_positive:
            article['rejection_reason'] = 'false positive (대학원/교육원/연수원/훈련원 강사)'
            rejected_articles.append(article)
            rejection_reasons['false_positive'] += 1
            continue
        
        # check for non-academic hagwons
        is_non_academic = any(keyword in combined_text for keyword in non_academic_keywords)
        if is_non_academic:
            article['rejection_reason'] = 'non-academic hagwon (art/music/dance/sports)'
            rejected_articles.append(article)
            rejection_reasons['non_academic'] += 1
            continue
        
        # check keywords
        found_keywords = []
        for keyword in keywords:
            if keyword in combined_text:
                found_keywords.append(keyword)
                keyword_counts[keyword] += 1
        
        # check for hagwon-related instructor terms
        has_hagwon_instructor = (
            '학원강사' in combined_text or 
            '학원 강사' in combined_text or
            '학원교사' in combined_text or
            '학원 교사' in combined_text
        )
        
        # track instructor types
        if '학원교사' in combined_text:
            instructor_types['학원교사'] += 1
        if '학원 교사' in combined_text:
            instructor_types['학원 교사'] += 1
        if '학원강사' in combined_text:
            instructor_types['학원강사'] += 1
        if '학원 강사' in combined_text:
            instructor_types['학원 강사'] += 1
        
        # double filter
        if not found_keywords:
            article['rejection_reason'] = 'missing keywords'
            rejected_articles.append(article)
            rejection_reasons['missing_keywords'] += 1
            continue
        
        if not has_hagwon_instructor:
            article['rejection_reason'] = 'missing instructor terms'
            article['found_keywords'] = found_keywords
            rejected_articles.append(article)
            rejection_reasons['missing_instructor_terms'] += 1
            continue
        
        # passed all filters
        combo_key = ' + '.join(sorted(found_keywords))
        combination_counts[combo_key] = combination_counts.get(combo_key, 0) + 1
        
        instructor_terms_found = []
        if '학원강사' in combined_text:
            instructor_terms_found.append('학원강사')
        if '학원 강사' in combined_text:
            instructor_terms_found.append('학원 강사')
        if '학원교사' in combined_text:
            instructor_terms_found.append('학원교사')
        if '학원 교사' in combined_text:
            instructor_terms_found.append('학원 교사')
        
        article['matched_keywords'] = found_keywords
        article['matched_instructor_terms'] = instructor_terms_found
        verified_articles.append(article)
    
    # print statistics
    print(f"\ntotal articles processed: {len(articles)}")
    print(f"verified (passed double filter): {len(verified_articles)}")
    print(f"rejected: {len(rejected_articles)}")
    if len(articles) > 0:
        print(f"acceptance rate: {len(verified_articles)/len(articles)*100:.1f}%")
    
    print(f"\nrejection breakdown:")
    for reason, count in rejection_reasons.items():
        print(f"  {reason}: {count}")
    
    print(f"\nindividual keyword counts:")
    for keyword, count in keyword_counts.items():
        print(f"  {keyword}: {count}")
    
    print(f"\nkeyword combinations:")
    for combo, count in sorted(combination_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {combo}: {count}")
    
    print(f"\ninstructor term counts:")
    for term, count in instructor_types.items():
        print(f"  {term}: {count}")
    
    return verified_articles, rejected_articles


def scrape_articles_batch(urls, scraped_urls, start_idx, end_idx, delay=1.0, save_interval=100):
    """
    scrape multiple articles with periodic saving
    """
    articles = []
    error_count = 0
    skipped_count = 0
    
    print(f"\nstarting batch scraping of {len(urls)} articles")
    print(f"already scraped: {len(scraped_urls)} urls")
    print("=" * 60)
    
    for i, url in enumerate(urls, 1):
        # skip if already scraped
        if url in scraped_urls:
            skipped_count += 1
            continue
        
        print(f"[{i}/{len(urls)}] scraping: {url[:60]}...")
        
        article = scrape_donga_article(url)
        
        if article and article.get('text'):
            articles.append(article)
            print(f"  ✓ saved (text: {len(article['text'])} chars)")
        else:
            error_count += 1
            print(f"  ✗ failed or no text")
        
        if len(articles) % save_interval == 0:
            checkpoint_file = f"checkpoint_donga_{start_idx}_{end_idx}.json"
            save_to_json(articles, checkpoint_file)
            print(f"\n→ checkpoint saved: {len(articles)} articles")
        
        if i % 50 == 0:
            print(f"\n--- progress: {i}/{len(urls)} ---")
            print(f"    saved: {len(articles)} | skipped: {skipped_count} | errors: {error_count}")
            print()
        
        time.sleep(delay)
    
    print("\n" + "=" * 60)
    print(f"scraping complete!")
    print(f"  total processed: {len(urls)}")
    print(f"  skipped: {skipped_count}")
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
    main execution function
    """
    # get range from command line arguments
    start_idx = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    end_idx = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    print("=" * 80)
    print("donga ilbo sitemap-based scraper with double filtering")
    print("boston university hagwon stigmatization research")
    if len(sys.argv) > 1:
        print(f"processing range: {start_idx} to {end_idx}")
    print("=" * 80)
    
    # configuration
    sitemap_url = "https://image.donga.com/sitemap/donga-sitemap.xml"
    
    delay_between_requests = 1.0
    save_checkpoint_every = 100
    
    output_dir = "donga_sitemap_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # step 1: load checkpoint for this range if exists
    print("\n step 1: loading checkpoint for this range")
    print("-" * 80)
    
    existing_articles, scraped_urls = load_checkpoint_for_range(start_idx, end_idx)
    
    # step 2: get urls from sitemap (filtered for 2005-2019)
    print("\n step 2: reading sitemap (filtering for 2005-2019)")
    print("-" * 80)
    
    all_urls = get_sitemap_urls(sitemap_url)
    
    if not all_urls:
        print("✗ no urls found in sitemap")
        return
    
    print(f"\ntotal urls collected: {len(all_urls)}")
    
    # slice urls based on command line arguments
    if end_idx:
        all_urls = all_urls[start_idx:end_idx]
        print(f"processing slice: {start_idx} to {end_idx} ({len(all_urls)} urls)")
    else:
        all_urls = all_urls[start_idx:]
        print(f"processing from {start_idx} to end ({len(all_urls)} urls)")
    
    print(f"already scraped: {len(scraped_urls)} urls")
    print(f"remaining: {len(all_urls) - len(scraped_urls)} urls")
    
    urls_file = os.path.join(output_dir, f"donga_sitemap_urls_{start_idx}_{end_idx}.txt")
    with open(urls_file, 'w', encoding='utf-8') as f:
        for url in all_urls:
            f.write(url + '\n')
    print(f"✓ saved {len(all_urls)} urls to {urls_file}")
    
    # step 3: scrape articles
    print("\n step 3: scraping articles")
    print("-" * 80)
    
    new_articles = scrape_articles_batch(
        urls=all_urls,
        scraped_urls=scraped_urls,
        start_idx=start_idx,
        end_idx=end_idx,
        delay=delay_between_requests,
        save_interval=save_checkpoint_every
    )
    
    # step 4: combine existing + new articles
    print("\n step 4: combining articles")
    print("-" * 80)
    
    all_articles = existing_articles + new_articles
    print(f"total articles: {len(all_articles)} (existing: {len(existing_articles)} + new: {len(new_articles)})")
    
    if not all_articles:
        print("✗ no articles scraped")
        return
    
    # step 5: double filter verification
    print("\n step 5: double filter verification")
    print("-" * 80)
    
    verified_articles, rejected_articles = verify_keywords_double_filter(all_articles)
    
    # step 6: save final results
    print("\n step 6: saving final results")
    print("-" * 80)
    
    # add range to filename
    range_suffix = f"_{start_idx}_{end_idx}" if end_idx else f"_{start_idx}_end"
    
    if verified_articles:
        json_file = os.path.join(output_dir, f"donga_hagwon_2005_2019_verified{range_suffix}.json")
        save_to_json(verified_articles, json_file)
        
        csv_file = os.path.join(output_dir, f"donga_hagwon_2005_2019_verified{range_suffix}.csv")
        save_to_csv(verified_articles, csv_file)
    
    if rejected_articles:
        rejected_file = os.path.join(output_dir, f"donga_hagwon_2005_2019_rejected{range_suffix}.json")
        save_to_json(rejected_articles, rejected_file)
    
    print("\n" + "=" * 80)
    print("✓ sitemap scraping completed!")
    print(f"  articles scraped: {len(all_articles)}")
    print(f"  articles verified: {len(verified_articles)}")
    print(f"  articles rejected: {len(rejected_articles)}")
    print(f"  date range: 2005-2019")
    print(f"  processing range: {start_idx} to {end_idx}")
    print(f"  output directory: {output_dir}/")
    print("=" * 80)


if __name__ == "__main__":
    main()