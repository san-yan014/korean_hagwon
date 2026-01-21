import json
import os
import sys
import pandas as pd
from datetime import datetime

def clean_articles(articles):
    """
    remove duplicates and filter by date range (2005-2019)
    """
    print("\n" + "=" * 80)
    print("cleaning articles: removing duplicates and filtering dates")
    print("=" * 80)
    
    initial_count = len(articles)
    print(f"initial article count: {initial_count}")
    
    # convert to dataframe for easier processing
    df = pd.DataFrame(articles)
    
    # step 1: remove duplicates by url
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['url'], keep='first')
    duplicates_removed = before_dedup - len(df)
    print(f"\nduplicates removed: {duplicates_removed}")
    print(f"after deduplication: {len(df)}")
    
    # step 2: filter by date range (2005-2019)
    before_date_filter = len(df)
    
    # parse dates and extract year
    df['parsed_date'] = pd.to_datetime(df['date'], errors='coerce')
    df['year'] = df['parsed_date'].dt.year
    
    # filter for 2005-2019
    df = df[(df['year'] >= 2005) & (df['year'] <= 2019)]
    
    outside_range = before_date_filter - len(df)
    print(f"\noutside date range (not 2005-2019): {outside_range}")
    print(f"after date filtering: {len(df)}")
    
    # remove temporary columns
    df = df.drop(columns=['parsed_date', 'year'])
    
    # convert back to list of dicts
    cleaned_articles = df.to_dict('records')
    
    print(f"\nfinal article count: {len(cleaned_articles)}")
    print(f"total removed: {initial_count - len(cleaned_articles)}")
    print("=" * 80)
    
    return cleaned_articles

def merge_checkpoints(checkpoint_dir, output_file):
    """
    merge all checkpoint files, remove duplicates, and filter dates
    """
    print("=" * 80)
    print("merging donga checkpoints")
    print("=" * 80)
    
    # find all checkpoint json files
    checkpoint_files = [f for f in os.listdir(checkpoint_dir) if f.startswith('checkpoint_donga_') and f.endswith('.json')]
    checkpoint_files.sort()
    
    print(f"\nfound {len(checkpoint_files)} checkpoint files")
    
    all_articles = []
    
    # load all checkpoints
    for i, filename in enumerate(checkpoint_files, 1):
        filepath = os.path.join(checkpoint_dir, filename)
        print(f"[{i}/{len(checkpoint_files)}] loading {filename}...")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                articles = json.load(f)
                all_articles.extend(articles)
                print(f"  loaded {len(articles)} articles")
        except Exception as e:
            print(f"  error: {e}")
    
    print(f"\ntotal articles before cleaning: {len(all_articles)}")
    
    # clean articles (dedup + date filter)
    cleaned_articles = clean_articles(all_articles)
    
    # save merged file
    print(f"\nsaving to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cleaned_articles, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ merge complete!")
    print(f"  input files: {len(checkpoint_files)}")
    print(f"  output file: {output_file}")
    print(f"  final articles: {len(cleaned_articles)}")
    print("=" * 80)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python merge.py <checkpoint_dir> <output_file>")
        print("example: python merge.py donga_checkpoints donga_merged.json")
        sys.exit(1)
    
    checkpoint_dir = sys.argv[1]
    output_file = sys.argv[2]
    
    merge_checkpoints(checkpoint_dir, output_file)