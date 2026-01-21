import os 

os.environ['ANTHROPIC_API_KEY'] ="###########"

"""
this script translates korean youtube comments to english using claude's batch api,
which provides 50% cost savings.

command line script instructions:
    input.csv   → korean comments to translate
    output.csv  → translated comments

basic usage:

dry run (test without submitting)
    python translate_comments.py input.csv output.csv --dry-run

submit full batch
    python translate_comments.py input.csv output.csv --submit-only

use existing translations (skip already translated)
    python translate_comments.py new_scraped.csv translated.csv --existing translated.csv --submit-only


process results later
    python translate_comments.py input.csv output.csv --process-only batch_abc123
    python translate_comments.py input.csv output.csv --process-only batch_abc123 --existing old_translated.csv

submit and wait for completion
    python translate_comments.py input.csv output.csv


what the script does:

    1. reads input.csv for korean comments
    2. loads existing translations (if --existing provided) to skip
    3. creates batch requests for new comments + unique channel names
    4. submits to anthropic's batch api
    5. saves batch metadata to batch_info_{batch_id}.json
    6. downloads results when batch completes
    7. merges new + existing translations to output.csv

input csv format:
    channel, video_url, text, author, date, likes

output csv format:
    channel, channel_translated, video_url, text, text_translated, author, date, likes
"""
import os
import json
import csv
import time
import argparse
from datetime import datetime
import anthropic



MODELS = {
    'sonnet': 'claude-sonnet-4-20250514',
    'haiku': 'claude-haiku-4-5-20251001'
}

# batch api costs per million tokens
COSTS = {
    'sonnet': {'input': 1.5, 'output': 7.5},
    'haiku': {'input': 0.4, 'output': 2.0}
}

SYSTEM_PROMPT = """You are a Korean-English translator specializing in Korean education contexts. Translate YouTube comments about hagwon (private academies) and education.

## TERMINOLOGY RULES

| Korean | English |
|--------|---------|
| 학원 (hakwon) | hagwon (never "academy" or "cram school") |
| 어학원 | language hagwon |
| 학원 강사 / 학원 교사 | hagwon instructor |
| 교사 / 선생님 (without 학원) | school teacher |
| 사교육 | hagwon education (not "private education") |
| 공교육 | public education |
| 수능 | CSAT (college entrance exam) |
| 내신 | school GPA |
| 입시 | college admissions |
| 과외 | private tutoring |
| 강남 | Gangnam |
| 대치동 | Daechi-dong |
| 스타강사 | star instructor |
| 재수생 | repeat test-taker |

## TRANSLATION PRINCIPLES

1. Preserve tone and sentiment (positive, negative, neutral)
2. Keep informal/colloquial style if present
3. Preserve slang, humor, sarcasm
4. Keep names in original format
5. Translate Korean internet slang naturally (ㅋㅋㅋ → "lol", ㅠㅠ → express sadness)

## OUTPUT FORMAT

Return ONLY a JSON object:
{"translated_text": "[translation here]"}"""




def load_csv(input_file):
    """load comments from csv"""
    comments = []
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            comments.append(row)
    return comments


def load_existing_translations(existing_file):
    """load already translated comments"""
    if not existing_file or not os.path.exists(existing_file):
        return {}
    
    translations = {}
    with open(existing_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # key by video_url + author + text prefix
            key = (row['video_url'], row['author'], row['text'][:50])
            translations[key] = {
                'text_translated': row.get('text_translated', ''),
                'channel_translated': row.get('channel_translated', '')
            }
    
    return translations


def create_batch_requests(comments, existing_translations=None, model='sonnet'):
    """create batch requests for comments and channel names"""
    requests = []
    existing_translations = existing_translations or {}
    model_name = MODELS[model]
    
    # get unique channel names
    channels = list(set(c['channel'] for c in comments))
    
    # filter out already translated channels
    existing_channels = set(v['channel_translated'] for v in existing_translations.values() if v.get('channel_translated'))
    channels_to_translate = [c for c in channels if c not in existing_channels]
    
    # create requests for channel names
    for i, channel in enumerate(channels_to_translate):
        request = {
            "custom_id": f"channel_{i}",
            "params": {
                "model": model_name,
                "max_tokens": 1000,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": f"Translate this Korean YouTube channel name to English. Return ONLY JSON: {{\"translated_text\": \"...\"}}\n\nChannel name: {channel}"}
                ]
            }
        }
        requests.append(request)
    
    # find comments that need translation
    comments_to_translate = []
    for i, comment in enumerate(comments):
        key = (comment['video_url'], comment['author'], comment['text'][:50])
        if key not in existing_translations:
            comments_to_translate.append((i, comment))
    
    # create requests for comments
    for i, comment in comments_to_translate:
        request = {
            "custom_id": f"comment_{i}",
            "params": {
                "model": model_name,
                "max_tokens": 64000,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": f"Translate this Korean YouTube comment to English. Return ONLY JSON: {{\"translated_text\": \"...\"}}\n\nComment: {comment['text']}"}
                ]
            }
        }
        requests.append(request)
    
    return requests, channels, channels_to_translate


def save_batch_requests(requests, output_file):
    """save batch requests to jsonl"""
    with open(output_file, 'w', encoding='utf-8') as f:
        for request in requests:
            f.write(json.dumps(request, ensure_ascii=False) + '\n')
    print(f"saved {len(requests)} requests to: {output_file}")


def submit_batch(client, requests_file):
    """submit batch job"""
    print("\nsubmitting batch...")
    
    requests_list = []
    with open(requests_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                requests_list.append(json.loads(line))
    
    batch = client.messages.batches.create(requests=requests_list)
    
    print(f"batch submitted: {batch.id}")
    print(f"status: {batch.processing_status}")
    
    return batch


def poll_batch_status(client, batch_id, poll_interval=60):
    """poll until complete"""
    print(f"\npolling (every {poll_interval}s)...")
    
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        counts = batch.request_counts
        total = counts.processing + counts.succeeded + counts.errored
        processed = counts.succeeded + counts.errored
        
        print(f"\r{batch.processing_status} | {processed}/{total} | ok:{counts.succeeded} err:{counts.errored}", end='', flush=True)
        
        if batch.processing_status == "ended":
            print("\nbatch complete!")
            return batch
        
        if batch.processing_status in ["canceled", "expired"]:
            print(f"\nbatch {batch.processing_status}")
            return batch
        
        time.sleep(poll_interval)


def download_results(client, batch, output_file):
    """download batch results"""
    print("\ndownloading results...")
    
    results_response = client.messages.batches.results(batch.id)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        for result in results_response:
            f.write(json.dumps(result.model_dump(), ensure_ascii=False) + '\n')
    
    print(f"saved to: {output_file}")
    return output_file


def parse_translation(result):
    """extract translation from result"""
    try:
        if result.get('result', {}).get('type') != 'succeeded':
            return None
        
        # check for truncation
        stop_reason = result.get('result', {}).get('message', {}).get('stop_reason')
        if stop_reason == 'max_tokens':
            return None
        
        content = result['result']['message']['content'][0]['text']
        
        # handle non-json responses (refusals)
        if not content.strip().startswith('{') and not content.strip().startswith('```'):
            return None
        
        # remove markdown wrapper
        if content.startswith("```"):
            lines = content.split("```")
            if len(lines) >= 2:
                content = lines[1]
                if content.startswith("json"):
                    content = content[4:]
            content = content.strip()
        
        # try standard json parsing first
        try:
            parsed = json.loads(content)
            return parsed.get('translated_text', '')
        except json.JSONDecodeError:
            # fallback: extract the translated_text value directly
            if '"translated_text":' in content:
                # find the start of the value
                start_marker = '"translated_text":'
                start_idx = content.index(start_marker) + len(start_marker)
                
                # skip whitespace and opening quote
                while start_idx < len(content) and content[start_idx] in ' \n\t':
                    start_idx += 1
                if start_idx < len(content) and content[start_idx] == '"':
                    start_idx += 1
                
                # find the end - look for closing quote followed by } or ,
                # but handle escaped quotes
                end_idx = start_idx
                while end_idx < len(content):
                    if content[end_idx] == '"':
                        # check if it's escaped
                        if end_idx > 0 and content[end_idx-1] != '\\':
                            # check if followed by } or end
                            next_char_idx = end_idx + 1
                            while next_char_idx < len(content) and content[next_char_idx] in ' \n\t':
                                next_char_idx += 1
                            if next_char_idx >= len(content) or content[next_char_idx] in '},':
                                break
                    end_idx += 1
                
                if end_idx > start_idx:
                    translation = content[start_idx:end_idx]
                    # unescape common sequences
                    translation = translation.replace('\\n', '\n').replace('\\t', '\t')
                    return translation
            
            return None
            
    except Exception as e:
        return None

def process_results(results_file, comments, channels, channels_to_translate, existing_translations=None):
    """process results and build output"""
    print("\nprocessing results...")
    
    existing_translations = existing_translations or {}
    
    # load results
    results = []
    if results_file and os.path.exists(results_file):
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    results.append(json.loads(line))
    
    # map results by custom_id
    results_map = {r['custom_id']: r for r in results}
    
    # process channel translations (new + existing)
    channel_translations = {}
    
    # first, get existing channel translations
    for key, val in existing_translations.items():
        if val.get('channel_translated'):
            # find which channel this belongs to
            for comment in comments:
                if (comment['video_url'], comment['author'], comment['text'][:50]) == key:
                    channel_translations[comment['channel']] = val['channel_translated']
                    break
    
    # then, get new channel translations
    for i, channel in enumerate(channels_to_translate):
        result = results_map.get(f"channel_{i}")
        if result:
            translation = parse_translation(result)
            channel_translations[channel] = translation or channel
        else:
            channel_translations[channel] = channel
    
    # fill in any missing channels
    for channel in channels:
        if channel not in channel_translations:
            channel_translations[channel] = channel
    
    # process comment translations
    output_rows = []
    error_rows = []
    success = 0
    reused = 0
    
    for i, comment in enumerate(comments):
        key = (comment['video_url'], comment['author'], comment['text'][:50])
        
        # check if already translated
        if key in existing_translations and existing_translations[key].get('text_translated'):
            translation = existing_translations[key]['text_translated']
            reused += 1
        else:
            result = results_map.get(f"comment_{i}")
            translation = parse_translation(result) if result else None
            
            if translation:
                success += 1
            else:
                # capture error details
                error_info = {
                    'index': i,
                    'text': comment['text'],
                    'author': comment['author'],
                    'video_url': comment['video_url'],
                    'error_type': 'no_result' if not result else 'parse_failed',
                    'raw_response': None
                }
                if result:
                    try:
                        error_info['raw_response'] = result['result']['message']['content'][0]['text'][:200]
                    except Exception as e:
                        print(f"error capturing raw response: {e}")
                        error_info['raw_response'] = str(result)[:200]
                error_rows.append(error_info)
                translation = "[translation failed]"
        
        row = {
            'channel': comment['channel'],
            'channel_translated': channel_translations.get(comment['channel'], comment['channel']),
            'video_url': comment['video_url'],
            'text': comment['text'],
            'text_translated': translation,
            'author': comment['author'],
            'date': comment['date'],
            'likes': comment['likes']
        }
        output_rows.append(row)
    
    print(f"new translations: {success}, reused: {reused}, errors: {len(error_rows)}")
    
    # save error log if any errors
    if error_rows:
        error_file = f"translation_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(error_file, 'w', encoding='utf-8') as f:
            json.dump(error_rows, f, ensure_ascii=False, indent=2)
        print(f"error details saved to: {error_file}")
    
    return output_rows

def save_csv(rows, output_file):
    """save results to csv"""
    if not rows:
        print("no rows to save")
        return
    
    fieldnames = ['channel', 'channel_translated', 'video_url', 'text', 'text_translated', 'author', 'date', 'likes']
    
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"saved {len(rows)} rows to: {output_file}")


def dry_run(comments, channels, requests, model='sonnet', existing_count=0):
    """show what would be submitted"""
    print("\n" + "=" * 50)
    print("dry run")
    print("=" * 50)
    
    new_comments = len([r for r in requests if r['custom_id'].startswith('comment_')])
    new_channels = len([r for r in requests if r['custom_id'].startswith('channel_')])
    
    print(f"\nmodel: {model}")
    print(f"\nwould translate:")
    print(f"  {new_channels} new channels")
    print(f"  {new_comments} new comments")
    print(f"  {existing_count} already translated (skipped)")
    print(f"  {len(requests)} total requests")
    
    # estimate cost
    avg_input = 300
    avg_output = 200
    total_input = len(requests) * avg_input
    total_output = len(requests) * avg_output
    cost = (total_input * COSTS[model]['input'] + total_output * COSTS[model]['output']) / 1_000_000
    
    print(f"\nestimated cost: ${cost:.2f}")


def main():
    parser = argparse.ArgumentParser(description='translate youtube comments using batch api')
    parser.add_argument('input_file', help='input csv')
    parser.add_argument('output_file', help='output csv')
    parser.add_argument('-k', '--api-key', help='anthropic api key')
    parser.add_argument('--existing', help='existing translated csv to skip already translated')
    parser.add_argument('--model', choices=['sonnet', 'haiku'], default='sonnet', help='model to use (default: sonnet)')
    parser.add_argument('--submit-only', action='store_true', help='submit and exit')
    parser.add_argument('--process-only', help='process existing batch (batch_id)')
    parser.add_argument('--poll-interval', type=int, default=60, help='poll interval seconds')
    parser.add_argument('--dry-run', action='store_true', help='show what would be submitted')
    
    args = parser.parse_args()
    
    api_key = args.api_key or os.environ.get('ANTHROPIC_API_KEY')
    if not api_key and not args.dry_run:
        print("api key required")
        return
    
    client = None if args.dry_run else anthropic.Anthropic(api_key=api_key)
    
    print("=" * 50)
    print("translate youtube comments")
    print("=" * 50)
    print(f"input:  {args.input_file}")
    print(f"output: {args.output_file}")
    print(f"model:  {args.model}")
    if args.existing:
        print(f"existing: {args.existing}")
    
    # load input
    comments = load_csv(args.input_file)
    print(f"\nloaded {len(comments)} comments")
    
    # load existing translations
    existing_translations = load_existing_translations(args.existing)
    if existing_translations:
        print(f"loaded {len(existing_translations)} existing translations")
    
    # get unique channels
    channels = list(set(c['channel'] for c in comments))
    print(f"found {len(channels)} unique channels")
    
    # process existing batch
    if args.process_only:
        batch = client.messages.batches.retrieve(args.process_only)
        if batch.processing_status != "ended":
            print(f"batch not ready: {batch.processing_status}")
            return
        
        results_file = f"batch_results_{args.process_only}.jsonl"
        download_results(client, batch, results_file)
        
        # need to recreate channels_to_translate for processing
        existing_channels = set(v['channel_translated'] for v in existing_translations.values() if v.get('channel_translated'))
        channels_to_translate = [c for c in channels if c not in existing_channels]
        
        output_rows = process_results(results_file, comments, channels, channels_to_translate, existing_translations)
        save_csv(output_rows, args.output_file)
        return
    
    # create requests
    requests, channels, channels_to_translate = create_batch_requests(comments, existing_translations, args.model)
    
    # count new comments to translate
    new_comments = len([r for r in requests if r['custom_id'].startswith('comment_')])
    print(f"need to translate: {new_comments} comments, {len(channels_to_translate)} channels")
    print(f"created {len(requests)} requests")
    
    if len(requests) == 0:
        print("\nall comments already translated!")
        # still need to output the file with existing translations
        output_rows = process_results(None, comments, channels, channels_to_translate, existing_translations)
        save_csv(output_rows, args.output_file)
        return
    
    if args.dry_run:
        dry_run(comments, channels, requests, args.model, len(existing_translations))
        return
    
    # save and submit
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    requests_file = f"batch_requests_{timestamp}.jsonl"
    save_batch_requests(requests, requests_file)
    
    batch = submit_batch(client, requests_file)
    
    # save batch info
    batch_info = {
        'batch_id': batch.id,
        'input_file': args.input_file,
        'output_file': args.output_file,
        'existing_file': args.existing,
        'model': args.model,
        'num_comments': len(comments),
        'num_new_comments': new_comments,
        'num_channels': len(channels)
    }
    with open(f"batch_info_{batch.id}.json", 'w') as f:
        json.dump(batch_info, f, indent=2)
    
    if args.submit_only:
        print(f"\nprocess later with: --process-only {batch.id}")
        return
    
    # poll and process
    batch = poll_batch_status(client, batch.id, args.poll_interval)
    
    results_file = f"batch_results_{batch.id}.jsonl"
    download_results(client, batch, results_file)
    
    output_rows = process_results(results_file, comments, channels, channels_to_translate, existing_translations)
    save_csv(output_rows, args.output_file)
    
    print("\ndone!")


if __name__ == "__main__":
    main()