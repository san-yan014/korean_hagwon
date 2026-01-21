"""
simple script to sample n amount of articles from a relevant_articles json file.
usage: 
    python sample.py relevant_articles.json sampled_articles.json -n 100
    python sample.py relevant_articles.json sampled_articles.json -n 100 --year 2011
"""
import json
import argparse
import random

def sample_articles(input_file, output_file, n, year=None):
    """sample n articles from json file"""
    
    print(f"loading articles from {input_file}...")
    
    # load articles
    with open(input_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    print(f"loaded {len(articles)} articles")
    
    # filter by year if specified
    if year:
        articles = [a for a in articles if a.get('date', '').startswith(str(year))]
        print(f"filtered to {len(articles)} articles from {year}")
    
    # check if n is valid
    if n > len(articles):
        print(f"warning: requested {n} articles but only {len(articles)} available")
        print(f"sampling all {len(articles)} articles")
        n = len(articles)
    
    # sample articles
    sampled = random.sample(articles, n)
    
    # add article numbers
    for i, article in enumerate(sampled, 1):
        article['article_number'] = i
    
    print(f"sampled {len(sampled)} articles")
    
    # save to output file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(sampled, f, ensure_ascii=False, indent=2)
    
    print(f"saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(
        description='sample n articles from json file'
    )
    parser.add_argument('input_file', help='input json file with articles')
    parser.add_argument('output_file', help='output json file for sampled articles')
    parser.add_argument('-n', '--num-samples', type=int, default=50,
                       help='number of articles to sample (default: 50)')
    parser.add_argument('--year', type=int, help='filter to only articles from this year')
    
    args = parser.parse_args()
    
    sample_articles(args.input_file, args.output_file, args.num_samples, args.year)

if __name__ == "__main__":
    main()