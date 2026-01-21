import json
import pandas as pd
import random
import time
from google.cloud import translate_v2 as translate
import os
import re

# set google application credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/project/koreateach/google_translate_key/llm-hagwon-ae2ee05ed40d.json'

def sampling(input_file, sampled_tracker_file, n=50):
    """randomly sample n unique articles that haven't been sampled before"""
    print("\n" + "=" * 60)
    print(f"sampling {n} articles from: {input_file}")
    print("=" * 60)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    print(f"loaded {len(articles)} articles")
    
    # load previously sampled urls
    previously_sampled = set()
    if os.path.exists(sampled_tracker_file):
        with open(sampled_tracker_file, 'r', encoding='utf-8') as f:
            previously_sampled = set(json.load(f))
        print(f"loaded {len(previously_sampled)} previously sampled urls")
    else:
        print("no previous samples found - starting fresh")
    
    # remove duplicates from input first
    seen_urls = set()
    unique_articles = []
    duplicate_count = 0
    already_sampled_count = 0
    
    for article in articles:
        url = article.get('url', '')
        if url in seen_urls:
            duplicate_count += 1
        elif url in previously_sampled:
            already_sampled_count += 1
        else:
            seen_urls.add(url)
            unique_articles.append(article)
    
    if duplicate_count > 0:
        print(f"\n⚠ removed {duplicate_count} duplicates from input file")
    if already_sampled_count > 0:
        print(f"⚠ removed {already_sampled_count} already sampled articles")
    print(f"working with {len(unique_articles)} unique unsampled articles")
    
    # check if we have enough articles
    if len(unique_articles) < n:
        print(f"\n✗ error: only {len(unique_articles)} unsampled articles available, need {n}")
        print("consider reducing sample size or using a different input file")
        return None
    
    # create dataframe
    df = pd.DataFrame(unique_articles)
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    
    # show year distribution
    year_counts = df['year'].value_counts().sort_index()
    print(f"\nyear distribution (available for sampling):")
    for year, count in year_counts.items():
        print(f"  {year}: {count} articles")
    
    # simple random sampling
    sample = df.sample(n=n, random_state=42)
    
    # show final distribution
    print(f"\nyear distribution (sampled):")
    final_year_counts = sample['year'].value_counts().sort_index()
    for year, count in final_year_counts.items():
        print(f"  {year}: {count} articles")
    
    print(f"\n✓ sampled {len(sample)} unique articles")
    
    # convert back to list of dicts
    sample_articles = sample.to_dict('records')
    
    # get sampled urls
    sampled_urls = [article['url'] for article in sample_articles]
    
    # update tracker file
    previously_sampled.update(sampled_urls)
    with open(sampled_tracker_file, 'w', encoding='utf-8') as f:
        json.dump(list(previously_sampled), f, ensure_ascii=False, indent=2)
    
    print(f"updated tracker file: {sampled_tracker_file}")
    print(f"total sampled so far: {len(previously_sampled)} articles")
    
    # convert datetime back to string for json serialization
    for article in sample_articles:
        if isinstance(article['date'], pd.Timestamp):
            article['date'] = article['date'].isoformat()
        if 'year' in article:
            del article['year']
    
    return sample_articles

def clean_text(text):
    """clean text by removing extra whitespace and html artifacts"""
    if not text:
        return ""
    
    # remove html tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # decode common html entities
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    text = text.replace('&apos;', "'")
    
    # remove common html/xml artifacts
    text = re.sub(r'&[a-zA-Z]+;', '', text)
    text = re.sub(r'&#\d+;', '', text)
    
    # remove zero-width spaces and other invisible characters
    text = text.replace('\u200b', '')
    text = text.replace('\ufeff', '')
    text = text.replace('\u00a0', ' ')
    
    # remove multiple spaces/newlines/tabs
    text = re.sub(r'\s+', ' ', text)
    
    # remove leading/trailing whitespace
    text = text.strip()
    
    return text

def translate_articles(articles, output_file):
    """translate korean articles to english using google translate"""
    print("\n" + "=" * 60)
    print("translating articles")
    print("=" * 60)
    
    # initialize google translate client
    translate_client = translate.Client()
    
    # custom terminology replacements
    custom_terms = {
        '학원': 'hagwon',
        '학원강사': 'hagwon teacher',
        '학원 강사': 'hagwon teacher',
        '학원교사': 'hagwon teacher',
        '학원 교사': 'hagwon teacher',
        '사교육': 'private education',
        '입시학원': 'test prep hagwon',
        '보습학원': 'supplementary hagwon'
    }
    
    translated_articles = []
    
    for i, article in enumerate(articles, 1):
        print(f"\ntranslating article {i}/{len(articles)}...")
        
        try:
            # clean original text (before translation)
            original_title = clean_text(article.get('title', ''))
            original_text = clean_text(article.get('text', ''))
            
            # translate title
            if original_title:
                translated_title = translate_client.translate(
                    original_title,
                    source_language='ko',
                    target_language='en'
                )['translatedText']
                
                # clean after translation to remove any html entities google added
                translated_title = clean_text(translated_title)
                
                # apply custom term replacements
                for korean, english in custom_terms.items():
                    # replace common mistranslations
                    translated_title = translated_title.replace('academy instructor', english)
                    translated_title = translated_title.replace('academy teacher', english)
                    translated_title = translated_title.replace('private academy', 'hagwon')
                    translated_title = translated_title.replace('cram school', 'hagwon')
            else:
                translated_title = ""
            
            # translate text
            if original_text:
                translated_text = translate_client.translate(
                    original_text,
                    source_language='ko',
                    target_language='en'
                )['translatedText']
                
                # clean after translation to remove any html entities google added
                translated_text = clean_text(translated_text)
                
                # apply custom term replacements
                for korean, english in custom_terms.items():
                    # replace common mistranslations
                    translated_text = translated_text.replace('academy instructor', english)
                    translated_text = translated_text.replace('academy teacher', english)
                    translated_text = translated_text.replace('private academy', 'hagwon')
                    translated_text = translated_text.replace('cram school', 'hagwon')
            else:
                translated_text = ""
            
            # create translated article
            translated_article = {
                'url': article.get('url', ''),
                'date': article.get('date', ''),
                'original_title': original_title,
                'translated_title': translated_title,
                'original_text': original_text,
                'translated_text': translated_text
            }
            
            # preserve matched keywords if they exist
            if 'matched_keywords' in article:
                translated_article['matched_keywords'] = article['matched_keywords']
            if 'matched_instructor_terms' in article:
                translated_article['matched_instructor_terms'] = article['matched_instructor_terms']
            
            translated_articles.append(translated_article)
            
            print(f"  ✓ title: {original_title[:50]}...")
            
            # rate limiting: wait between requests
            time.sleep(0.1)
            
        except Exception as e:
            print(f"  ✗ error translating article {i}: {e}")
            # save original without translation on error
            translated_article = {
                'url': article.get('url', ''),
                'date': article.get('date', ''),
                'original_title': clean_text(article.get('title', '')),
                'translated_title': "[translation failed]",
                'original_text': clean_text(article.get('text', '')),
                'translated_text': "[translation failed]",
                'error': str(e)
            }
            translated_articles.append(translated_article)
    
    # save translated articles
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(translated_articles, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ translated {len(translated_articles)} articles")
    print(f"saved to: {output_file}")
    
    return translated_articles

def main():
    # create output directory if it doesn't exist
    os.makedirs("donga_translation", exist_ok=True)
    
    # find the next sample number
    import glob
    existing_samples = glob.glob("donga_translation/donga_sample_*_translated.json")
    if existing_samples:
        # extract numbers from filenames
        numbers = []
        for filepath in existing_samples:
            # extract number from filename like "donga_sample_2_translated.json"
            filename = os.path.basename(filepath)
            try:
                num = int(filename.split('_')[2])
                numbers.append(num)
            except (IndexError, ValueError):
                continue
        next_num = max(numbers) + 1 if numbers else 1
    else:
        next_num = 1
    
    # file paths
    input_file = "donga_filtered_output/donga_verified_articles.json"
    sampled_tracker_file = "donga_translation/sampled_urls.json"
    translated_file = f"donga_translation/donga_sample_{next_num}_translated.json"
    
    print(f"\nthis will be sample number: {next_num}")
    
    # step 1: sample 50 articles
    print("\nstep 1: sampling articles")
    sample_articles = sampling(input_file, sampled_tracker_file, n=50)
    
    if sample_articles is None:
        print("\n✗ sampling failed - exiting")
        return
    
    # step 2: translate articles
    print("\nstep 2: translating articles")
    translated_articles = translate_articles(sample_articles, translated_file)
    
    print("\n" + "=" * 80)
    print("processing complete!")
    print("=" * 80)
    print(f"\noutput file:")
    print(f"  translated: {translated_file}")

if __name__ == "__main__":
    main()