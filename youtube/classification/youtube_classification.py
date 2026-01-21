import os 

os.environ['ANTHROPIC_API_KEY'] ="################"


"""
this script classifies translated youtube comments about hagwon instructors using claude's batch api.
analyzes how comments portray hagwon teachers across 16 stigmatization categories.

command line script instructions:
    input.csv   → translated comments to classify
    output.csv  → classified comments with codes

input csv format:
    channel, channel_translated, video_url, text, text_translated, author, date, likes

output csv format:
    channel, channel_translated, video_url, text, text_translated, author, date, likes, code, code_5_sub, justification

----------------------------------------------------------------------------------------------------------------------

basic usage:

dry run (test without submitting)
    python classify_comments.py input.csv output.csv --dry-run
    python classify_comments.py input.csv output.csv --dry-run --model haiku

submit batch only
    python classify_comments.py input.csv output.csv --submit-only
    python classify_comments.py input.csv output.csv --submit-only --model haiku

use existing classifications (skip already classified)
    python classify_comments.py new_translated.csv classified.csv --existing old_classified.csv
    python classify_comments.py new_translated.csv classified.csv --existing old_classified.csv --submit-only

process results later
    python classify_comments.py input.csv output.csv --process-only batch_abc123
    python classify_comments.py input.csv output.csv --process-only batch_abc123 --existing old_classified.csv

submit and wait for completion
    python classify_comments.py input.csv output.csv
    python classify_comments.py input.csv output.csv --model haiku

----------------------------------------------------------------------------------------------------------------------

what the script does:

    1. reads input.csv for translated comments
    2. loads existing classifications (if --existing provided) to skip
    3. creates batch requests for new comments using 16-code stigmatization framework
    4. submits to anthropic's batch api (50% cost savings vs standard api)
    5. saves batch metadata to batch_info_{batch_id}.json
    6. downloads results when batch completes
    7. merges new + existing classifications to output.csv

model options:
    --model sonnet (default, more accurate)
    --model haiku (faster, cheaper)


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

CODEBOOK = """
you are analyzing korean youtube comments about hagwon (private academy) instructors. classify each comment according to the following codebook:

CODE 1: FACILITATOR OF ACADEMIC DISHONESTY
the comment portrays hagwon teachers as helping students cheat or engage in academic fraud.

CODE 2: CONDUIT OF SOCIAL INEQUITY
the comment portrays hagwon teachers as agents who reproduce socioeconomic inequality by providing advantages exclusively to wealthy families.

CODE 3: PROFIT MOTIVE DOUBTED
the comment casts doubt on hagwon teachers' commitment to education, suggesting primary motivation is financial gain.

CODE 4: OPPOSED AS UNDERMINING PUBLIC EDUCATION
the comment portrays hagwon teachers/industry as undermining public education.

CODE 5: CRIMINAL CONDUCT
the comment portrays hagwon teachers as perpetrators of criminal offenses.
subcategories: (a) sexual misconduct with students, (b) sexual misconduct with non-students, (c) drug-related crimes, (d) violent crimes or financial fraud.

CODE 6: UNQUALIFIED / SUBSTANDARD CREDENTIALS
the comment portrays hagwon teachers as lacking proper qualifications or credentials.

CODE 7: PRECARIOUS / MARGINAL EMPLOYMENT
the comment portrays hagwon teaching as economically insecure or low-paid work.

CODE 8: VICTIM OF DEFAMATION / FALSE ACCUSATIONS
the comment portrays a hagwon teacher as having been wrongly accused or defamed.

CODE 9: FALLBACK CAREER FOR THE EDUCATED
the comment portrays hagwon teaching as a last-resort occupation for educated individuals who failed to secure better employment.

CODE 10: TRANSITIONAL OR PART-TIME WORK
the comment portrays hagwon teaching as temporary employment while pursuing other goals.

CODE 11: SUGGESTED AFFORDABILITY
the comment portrays hagwon teachers' services as accessible or affordable.

CODE 12: EDUCATIONALLY COUNTERPRODUCTIVE
the comment portrays hagwon education as ineffective or harmful to students' learning/wellbeing.

CODE 13: FUNCTIONAL EDUCATIONAL SERVICE
the comment portrays hagwon teachers neutrally as providers of standard educational services.

CODE 14: RECOGNIZED EXPERT / PROFESSIONAL PEER
the comment portrays hagwon teachers as legitimate professionals with expertise and credibility. includes comments saying the lectures/videos are helpful, educational, or effective for learning.

CODE 15: GLAMOROUS / HIGH-EARNING WORKER
the comment portrays hagwon teachers as exceptionally successful, high-earning, or celebrity-like.

CODE 16: ORDINARY CITIZEN
the comment mentions a hagwon teacher incidentally as an everyday member of society.

CODE 0: NOT APPLICABLE
the comment does not contain any portrayal of hagwon instructors that fits the above categories, or is too vague/unclear to classify.
"""

CLASSIFICATION_PROMPT = """classify this youtube comment about hagwon instructors.

comment: {text}

instructions:
1. assign the single most applicable code (0-16)
2. for code 5, specify subcategory (a, b, c, or d)
3. provide brief justification

return ONLY a json object:
{{
  "code": 13,
  "code_5_sub": null,
  "justification": "brief explanation"
}}"""


def load_csv(input_file):
    """load comments from csv"""
    comments = []
    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            comments.append(row)
    return comments


def load_existing_classifications(existing_file):
    """load already classified comments"""
    if not existing_file or not os.path.exists(existing_file):
        return {}
    
    classifications = {}
    with open(existing_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # key by video_url + author + text prefix
            key = (row['video_url'], row['author'], row['text'][:50])
            classifications[key] = {
                'code': row.get('code', ''),
                'code_5_sub': row.get('code_5_sub', ''),
                'justification': row.get('justification', '')
            }
    
    return classifications


def create_batch_requests(comments, existing_classifications=None, model='sonnet'):
    """create batch requests for classification"""
    requests = []
    existing_classifications = existing_classifications or {}
    model_name = MODELS[model]
    
    # find comments that need classification
    comments_to_classify = []
    for i, comment in enumerate(comments):
        key = (comment['video_url'], comment['author'], comment['text'][:50])
        if key not in existing_classifications:
            comments_to_classify.append((i, comment))
    
    # create requests for comments
    for i, comment in comments_to_classify:
        text = comment.get('text_translated', '')
        
        request = {
            "custom_id": f"comment_{i}",
            "params": {
                "model": model_name,
                "max_tokens": 5000,
                "system": CODEBOOK,
                "messages": [
                    {"role": "user", "content": CLASSIFICATION_PROMPT.format(text=text)}
                ]
            }
        }
        requests.append(request)
    
    return requests


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


def parse_classification(result):
    """extract classification from result"""
    try:
        if result.get('result', {}).get('type') != 'succeeded':
            return None
        
        content = result['result']['message']['content'][0]['text']
        
        # remove markdown wrapper
        if content.startswith("```"):
            lines = content.split("```")
            if len(lines) >= 2:
                content = lines[1]
                if content.startswith("json"):
                    content = content[4:]
            content = content.strip()
        
        # try standard json parsing
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # fallback: extract key fields manually
            classification = {}
            
            # extract code
            if '"code":' in content:
                try:
                    start = content.index('"code":') + len('"code":')
                    end = start
                    while end < len(content) and content[end] in ' \n\t':
                        end += 1
                    num_start = end
                    while end < len(content) and content[end].isdigit():
                        end += 1
                    if end > num_start:
                        classification['code'] = int(content[num_start:end])
                except:
                    pass
            
            # extract code_5_sub
            if '"code_5_sub":' in content:
                try:
                    start = content.index('"code_5_sub":') + len('"code_5_sub":')
                    if 'null' in content[start:start+10]:
                        classification['code_5_sub'] = None
                    elif '"' in content[start:start+20]:
                        quote_start = content.index('"', start) + 1
                        quote_end = content.index('"', quote_start)
                        classification['code_5_sub'] = content[quote_start:quote_end]
                except:
                    classification['code_5_sub'] = None
            
            # extract justification
            if '"justification":' in content:
                try:
                    start = content.index('"justification":') + len('"justification":')
                    quote_start = content.index('"', start) + 1
                    quote_end = quote_start
                    while quote_end < len(content):
                        if content[quote_end] == '"' and content[quote_end-1] != '\\':
                            break
                        quote_end += 1
                    classification['justification'] = content[quote_start:quote_end]
                except:
                    classification['justification'] = ''
            
            return classification if classification else None
            
    except Exception as e:
        print(f"parse error: {e}")
        return None


def process_results(results_file, comments, existing_classifications=None):
    """process results and build output"""
    print("\nprocessing results...")
    
    existing_classifications = existing_classifications or {}
    
    # load results
    results = []
    if results_file and os.path.exists(results_file):
        with open(results_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    results.append(json.loads(line))
    
    # map results by custom_id
    results_map = {r['custom_id']: r for r in results}
    
    # process classifications
    output_rows = []
    success = 0
    reused = 0
    errors = 0
    
    for i, comment in enumerate(comments):
        key = (comment['video_url'], comment['author'], comment['text'][:50])
        
        # check if already classified
        if key in existing_classifications and existing_classifications[key].get('code'):
            code = existing_classifications[key]['code']
            code_5_sub = existing_classifications[key].get('code_5_sub', '')
            justification = existing_classifications[key].get('justification', '')
            reused += 1
        else:
            result = results_map.get(f"comment_{i}")
            classification = parse_classification(result) if result else None
            
            if classification:
                success += 1
                code = classification.get('code')
                code_5_sub = classification.get('code_5_sub')
                justification = classification.get('justification', '')
            else:
                errors += 1
                code = None
                code_5_sub = None
                justification = "[classification failed]"
        
        row = {
            'channel': comment.get('channel', ''),
            'channel_translated': comment.get('channel_translated', ''),
            'video_url': comment.get('video_url', ''),
            'text': comment.get('text', ''),
            'text_translated': comment.get('text_translated', ''),
            'author': comment.get('author', ''),
            'date': comment.get('date', ''),
            'likes': comment.get('likes', ''),
            'code': code,
            'code_5_sub': code_5_sub if code_5_sub else '',
            'justification': justification
        }
        output_rows.append(row)
    
    print(f"new classifications: {success}, reused: {reused}, errors: {errors}")
    
    return output_rows


def save_csv(rows, output_file):
    """save results to csv"""
    if not rows:
        print("no rows to save")
        return
    
    fieldnames = [
        'channel', 'channel_translated', 'video_url', 'text', 'text_translated',
        'author', 'date', 'likes', 'code', 'code_5_sub', 'justification'
    ]
    
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"saved {len(rows)} rows to: {output_file}")


def dry_run(comments, requests, model='sonnet', existing_count=0):
    """show what would be submitted"""
    print("\n" + "=" * 50)
    print("dry run")
    print("=" * 50)
    
    print(f"\nmodel: {model}")
    print(f"\nwould classify:")
    print(f"  {len(requests)} new comments")
    print(f"  {existing_count} already classified (skipped)")
    
    # estimate cost
    avg_input = 800  # codebook + prompt
    avg_output = 100
    total_input = len(requests) * avg_input
    total_output = len(requests) * avg_output
    cost = (total_input * COSTS[model]['input'] + total_output * COSTS[model]['output']) / 1_000_000
    
    print(f"\nestimated cost: ${cost:.2f}")


def main():
    parser = argparse.ArgumentParser(description='classify youtube comments using batch api')
    parser.add_argument('input_file', help='input csv (translated comments)')
    parser.add_argument('output_file', help='output csv')
    parser.add_argument('-k', '--api-key', help='anthropic api key')
    parser.add_argument('--existing', help='existing classified csv to skip already classified')
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
    print("classify youtube comments")
    print("=" * 50)
    print(f"input:  {args.input_file}")
    print(f"output: {args.output_file}")
    print(f"model:  {args.model}")
    if args.existing:
        print(f"existing: {args.existing}")
    
    # load input
    comments = load_csv(args.input_file)
    print(f"\nloaded {len(comments)} comments")
    
    # load existing classifications
    existing_classifications = load_existing_classifications(args.existing)
    if existing_classifications:
        print(f"loaded {len(existing_classifications)} existing classifications")
    
    # process existing batch
    if args.process_only:
        batch = client.messages.batches.retrieve(args.process_only)
        if batch.processing_status != "ended":
            print(f"batch not ready: {batch.processing_status}")
            return
        
        results_file = f"batch_results_{args.process_only}.jsonl"
        download_results(client, batch, results_file)
        
        output_rows = process_results(results_file, comments, existing_classifications)
        save_csv(output_rows, args.output_file)
        return
    
    # create requests
    requests = create_batch_requests(comments, existing_classifications, args.model)
    
    print(f"need to classify: {len(requests)} comments")
    
    if len(requests) == 0:
        print("\nall comments already classified!")
        output_rows = process_results(None, comments, existing_classifications)
        save_csv(output_rows, args.output_file)
        return
    
    if args.dry_run:
        dry_run(comments, requests, args.model, len(existing_classifications))
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
        'num_new_comments': len(requests)
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
    
    output_rows = process_results(results_file, comments, existing_classifications)
    save_csv(output_rows, args.output_file)
    
    print("\ndone!")


if __name__ == "__main__":
    main()