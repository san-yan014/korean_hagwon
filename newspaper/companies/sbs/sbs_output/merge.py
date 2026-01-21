import json

def merge_articles():
    all_articles = []
    
    for year in range(2005, 2020):
        filename = f'sbs_{year}_articles.json'
        
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                year_articles = json.load(f)
                all_articles.extend(year_articles)
                print(f"{year}: {len(year_articles)} articles")
        except FileNotFoundError:
            print(f"{year}: file not found, skipping")
    
    # save merged file
    with open('sbs_all_articles_2005_2019.json', 'w', encoding='utf-8') as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)
    
    print(f"\ntotal articles merged: {len(all_articles)}")
    print("saved to: sbs_all_articles_2005_2019.json")

if __name__ == '__main__':
    merge_articles()