"""
latest edition

usage: python eunice_filter.py input.json text_field_name [options]
example: python eunice_filter.py articles.json original_text
example: python eunice_filter.py articles.json original_text -o results/output.json
example: python eunice_filter.py articles.json translated_text -o filtered.json -p "DongA"

simple double filter: articles must have keywords AND hagwon instructor terms
excludes false positives like 대학원 강사, 교육원 강사, etc.

steps: 

1. checks if article contains any of the keywords from the lists to exclude (false_positive_patterns, non_academic_keywords, obituary patterns) 
2. checks if the article contains the hagwon keywords 
3. checks if the explicit hagwon instructor terms are included - required (if article passes, NEEDS to pass this in order to be included otherwise, irrelevant articles pass)

usage is shown above. this code will also inlcude handling duplicates. remove articles that are out of the 2005-2019 range. 
make sure every article is in the correct date format. check if the article contains a publication if not then, it also allows the user to write the publication name.
"""
import json
import sys
import os
import argparse
import pandas as pd
import re
from datetime import datetime

# ✅ keep these
KEYWORDS = [
    '학원',      # hagwon
    '사교육'     # hagwon education
]

# MAYBE MAKE HAGWON INSTRUCTOR IN THE TOP LIST 
HAGWON_SPECIFIC = [
    '학원가',        # hagwon district/area
    '학원 시장',     # hagwon market
    '학원업계',      # hagwon industry
    '학원 운영',     # hagwon operation
    '학원비',        # hagwon fees
    '수강료',        # tuition/course fees
    '학원 등록',     # hagwon enrollment
    '입시학원',      # exam prep hagwon
    '대형학원',      # large hagwon
    '스타강사',      # star instructor (hagwon context)
    '온라인 강의',   # online lectures (often hagwon)
    '인터넷 강의',   # internet lectures
    '학원 강사',     # hagwon instructor (explicit)
    '학원강사',      # hagwon instructor (no space)
    '학원 교사',     # hagwon teacher (explicit)
    '학원교사',      # hagwon teacher (no space)
    '보습학원',      # tutoring hagwon
    '입시',          # exam preparation (should have excluded this)
]

#  OPTIONAL - if present, article is more likely relevant
INSTRUCTOR_TERMS = [
    '강사',      # instructor
    '교사',      # teacher  
    '선생님',    # teacher (honorific)
    '강의',      # lecture
    '수업',      # class/lesson
]

# false positive patterns to exclude
FALSE_POSITIVE_PATTERNS = [
    '대학원 강사',      # graduate school instructor (with space)
    '대학원강사',       # graduate school instructor (no space)
    '교육원 강사',      # education center instructor (with space)
    '교육원강사',       # education center instructor (no space)
    '연수원 강사',      # training center instructor (with space)
    '연수원강사',       # training center instructor (no space)
    '훈련원 강사',      # training institute instructor (with space)
    '훈련원강사',       # training institute instructor (no space)
    '문화원 강사',      # culture center instructor (with space)
    '문화원강사',       # culture center instructor (no space)
    '평생교육원 강사',   # lifelong education center instructor
    '직업훈련원 강사'    # vocational training center instructor
]

# non-academic hagwon keywords to exclude
NON_ACADEMIC_KEYWORDS = [
    '미술학원',    # art academy (no space)
    '미술 학원',   # art academy (with space)
    '화실',       # art studio
    '음악학원',    # music academy (no space)
    '음악 학원',   # music academy (with space)
    '피아노학원',  # piano academy (no space)
    '피아노 학원', # piano academy (with space)
    '댄스학원',    # dance academy (no space)
    '댄스 학원',   # dance academy (with space)
    '발레',       # ballet
    '방송댄스',    # broadcast dance / street dance
    '체육학원',    # sports academy (no space)
    '체육 학원',   # sports academy (with space)
    '태권도',     # taekwondo
    '유도',       # judo
    '검도',       # kendo / kumdo
    '수영',       # swimming
    '축구',       # soccer
    '골프',       # golf
    '연기학원',    # acting academy (no space)
    '연기 학원',   # acting academy (with space)
    '뮤지컬',     # musical theater
    '요가학원',    # yoga academy
    '요가 학원',    # yoga academy (with space)
    '필라테스',     # pilates
    '문화학원',    # culture academy (no space)
    '문화 학원'    # culture academy (with space)
]

def convert_date_format(date_string):
    # convert various date formats to iso format
    if not date_string:
        return date_string
    
    date_str = str(date_string)
    
    # already in iso format with timezone - keep as is
    if 'T' in date_str and ('+' in date_str or 'Z' in date_str):
        return date_str
    
    # sbs format: "작성2005.12.12 10:01조회조회수"
    match = re.search(r'작성(\d{4})\.(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})', date_str)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)
        day = match.group(3).zfill(2)
        hour = match.group(4).zfill(2)
        minute = match.group(5)
        return f"{year}-{month}-{day}T{hour}:{minute}:00+09:00"
    
    # yna format: 20191229154508 (yyyymmddhhmmss)
    if len(date_str) == 14 and date_str.isdigit():
        year = date_str[0:4]
        month = date_str[4:6]
        day = date_str[6:8]
        hour = date_str[8:10]
        minute = date_str[10:12]
        second = date_str[12:14]
        return f"{year}-{month}-{day}T{hour}:{minute}:{second}+09:00"
    
    # korean format: "2025년 12월 18일(목)"
    match = re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', date_str)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)
        day = match.group(3).zfill(2)
        return f"{year}-{month}-{day}T00:00:00+09:00"
    
    # already in iso format without timezone - add timezone
    if 'T' in date_str and '+' not in date_str and 'Z' not in date_str:
        return f"{date_str}+09:00"
    
    return date_str

def extract_year_from_date(date_string):
    # extract year from date string
    if not date_string:
        return None
    
    date_str = str(date_string)
    
    # iso format
    if 'T' in date_str or '-' in date_str:
        try:
            year = int(date_str[:4])
            return year
        except:
            pass
    
    # yna format: 20191229154508
    if len(date_str) == 14 and date_str.isdigit():
        return int(date_str[:4])
    
    # korean format: "2025년 12월 18일"
    match = re.search(r'(\d{4})년', date_str)
    if match:
        return int(match.group(1))
    
    return None

def normalize_dates(df):
    # normalize date formats in dataframe
    if 'date' in df.columns:
        df['date'] = df['date'].apply(convert_date_format)
    
    if 'datetime' in df.columns:
        df['datetime'] = df['datetime'].apply(convert_date_format)
    
    return df

def classify_article(text, title=""):
    combined_text = title + ' ' + text
    
    # exclusions
    for pattern in FALSE_POSITIVE_PATTERNS:
        if pattern in combined_text:
            return False, f"false positive: {pattern}"
    
    for pattern in NON_ACADEMIC_KEYWORDS:
        if pattern in combined_text:
            return False, f"non-academic: {pattern}"
    
    # requirement 1: must have hagwon keywords
    found_keywords = []
    if '학원' in combined_text:
        found_keywords.append('학원')
    if '사교육' in combined_text:
        found_keywords.append('사교육')

    if not found_keywords:
        return False, "no hagwon keywords"

    # requirement 2: EITHER have hagwon-specific terms OR instructor terms
    found_specific = [t for t in HAGWON_SPECIFIC if t in combined_text]
    found_instructor = [t for t in INSTRUCTOR_TERMS if t in combined_text]

    # option a: strict - must have BOTH
    if not found_specific or not found_instructor:
        return False, "missing hagwon-specific or instructor terms"

    # build reason showing both keyword and specific terms
    keyword_part = ', '.join(found_keywords)
    specific_part = ', '.join(found_specific[:2])
    return True, f"verified: {keyword_part} + {specific_part}"

# def classify_article(text, title=""):
#     """
#     simple double filter: must have keywords AND hagwon instructor terms
#     """
#     # combine title and text
#     combined_text = title + ' ' + text
    
#     # exclusion 1: false positive patterns
#     for pattern in FALSE_POSITIVE_PATTERNS:
#         if pattern in combined_text:
#             return False, f"false positive: {pattern}"
    
#     # exclusion 2: non-academic hagwons
#     found_non_academic = [kw for kw in NON_ACADEMIC_KEYWORDS if kw in combined_text]
#     if found_non_academic:
#         return False, f"non-academic: {', '.join(found_non_academic[:3])}"
    
#     # requirement 1: must have keywords
#     found_keywords = [kw for kw in KEYWORDS if kw in combined_text]
#     if not found_keywords:
#         return False, "missing keywords"
    
#     # requirement 2: must have instructor terms
#     found_instructor_terms = [term for term in INSTRUCTOR_TERMS if term in combined_text]
#     if not found_instructor_terms:
#         return False, "missing instructor terms"
    
#     # passed all filters
#     return True, f"verified: {', '.join(found_instructor_terms)}"

def main():
    parser = argparse.ArgumentParser(
        description='filter hagwon articles with simple double-filter logic'
    )
    parser.add_argument('input_file', help='input json file with articles')
    parser.add_argument('text_field', help='name of field containing article text')
    parser.add_argument('-o', '--output', default='filtered_verified.json',
                       help='output file path (default: filtered_verified.json)')
    parser.add_argument('-p', '--publication', help='publication name to add if missing')
    parser.add_argument('--save-excluded', action='store_true',
                       help='also save excluded articles to separate file')
    parser.add_argument('--csv', action='store_true',
                       help='also save as csv file')
    
    args = parser.parse_args()
    
    # create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # load json file
    print(f"loading {args.input_file}...")
    with open(args.input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # convert to dataframe
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                df = pd.DataFrame(value)
                break
        else:
            df = pd.DataFrame([data])
    
    print(f"loaded {len(df)} articles")
    
    # handle publication field
    if 'publication' not in df.columns:
        print("\n⚠ warning: no 'publication' field found")
        if args.publication:
            publication = args.publication
        else:
            publication = input("enter publication name to add: ").strip()
        
        if publication:
            df['publication'] = publication
            print(f"added publication '{publication}' to all articles")
    else:
        missing_pub = df['publication'].isna().sum()
        if missing_pub > 0:
            print(f"\n⚠ warning: {missing_pub} articles missing publication")
            if args.publication:
                publication = args.publication
            else:
                publication = input(f"enter publication for {missing_pub} articles: ").strip()
            
            if publication:
                df.loc[df['publication'].isna(), 'publication'] = publication
                print(f"added publication '{publication}' to {missing_pub} articles")
    
    # remove duplicates
    initial_count = len(df)
    duplicates_removed = 0
    if 'url' in df.columns:
        df = df.drop_duplicates(subset=['url'], keep='first')
        duplicates_removed = initial_count - len(df)
        if duplicates_removed > 0:
            print(f"removed {duplicates_removed} duplicates")
    
    # normalize dates
    print("normalizing dates...")
    df = normalize_dates(df)
    
    # filter by date range (2005-2019)
    print("filtering by date range (2005-2019)...")
    date_field = 'date' if 'date' in df.columns else 'datetime' if 'datetime' in df.columns else None
    
    outside_range = 0
    before_filter = len(df)
    if date_field:
        df['year'] = df[date_field].apply(extract_year_from_date)
        df = df[(df['year'] >= 2005) & (df['year'] <= 2019)]
        outside_range = before_filter - len(df)
        
        if outside_range > 0:
            print(f"removed {outside_range} articles outside 2005-2019")
        
        df = df.drop(columns=['year'])
    else:
        print("⚠ warning: no date field found, skipping date filtering")
    
    print(f"processing {len(df)} articles")
    
    # check text field exists
    if args.text_field not in df.columns:
        print(f"error: field '{args.text_field}' not found")
        print(f"available fields: {list(df.columns)}")
        sys.exit(1)
    
    include_list = []
    reason_list = []
    
    # check for title field
    title_field = None
    for field in ['title', 'original_title']:
        if field in df.columns:
            title_field = field
            break
    
    for idx, row in df.iterrows():
        title = row[title_field] if title_field and pd.notna(row[title_field]) else ""
        text = row[args.text_field] if pd.notna(row[args.text_field]) else ""
        include, reason = classify_article(text, title)
        include_list.append(include)
        reason_list.append(reason)
    
    df["include"] = include_list
    df["reason"] = reason_list
    
    # split verified and rejected
    verified = df[df["include"] == True]
    rejected = df[df["include"] == False]
    
    # save verified articles as json
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(verified.to_dict('records'), f, ensure_ascii=False, indent=2)
    
    print(f"\nverified articles saved to: {args.output}")
    
    # save as csv if requested
    if args.csv and len(verified) > 0:
        csv_file = args.output.replace('.json', '.csv')
        verified_csv = verified.copy()
        
        # flatten any list columns for csv
        for col in verified_csv.columns:
            if verified_csv[col].apply(lambda x: isinstance(x, list)).any():
                verified_csv[col] = verified_csv[col].apply(
                    lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x
                )
        
        verified_csv.to_csv(csv_file, index=False, encoding='utf-8-sig')
        print(f"csv file saved to: {csv_file}")
    
    # save excluded if requested
    if args.save_excluded and len(rejected) > 0:
        excluded_file = args.output.replace('.json', '_excluded.json')
        with open(excluded_file, 'w', encoding='utf-8') as f:
            json.dump(rejected.to_dict('records'), f, ensure_ascii=False, indent=2)
        print(f"excluded articles saved to: {excluded_file}")
    
    # print summary
    print("\n" + "="*50)
    print("results summary")
    print("="*50)
    print(f"original articles: {initial_count}")
    print(f"after deduplication: {initial_count - duplicates_removed}")
    print(f"after date filtering: {before_filter - outside_range}")
    print(f"verified: {len(verified)}")
    print(f"rejected: {len(rejected)}")
    
    if len(verified) > 0:
        acceptance_rate = (len(verified) / len(df)) * 100
        print(f"acceptance rate: {acceptance_rate:.1f}%")
    
    # if len(rejected) > 0:
    #     print("\nrejection breakdown:")
    #     rejection_reasons = rejected["reason"].value_counts()
    #     for reason, count in rejection_reasons.items():
    #         print(f"  - {reason}: {count}")
    
    # if len(verified) > 0:
    #     print("\nverification breakdown:")
    #     verification_reasons = verified["reason"].value_counts()
    #     for reason, count in verification_reasons.items():
    #         print(f"  - {reason}: {count}")
    
    print("\n" + "="*50)
    print("filtering complete!")
    print("="*50)

if __name__ == "__main__":
    main()