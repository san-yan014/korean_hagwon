"""
This script translates Korean news articles to English using Claude's Batch API, which provides 50% cost savings. It uses a three-file system to safely manage translations and prevent data loss.

Command line script instructions: 
    input.json        → All Korean articles to translate (never modified)
    in_progress.json  → Already translated articles (read-only, checked for duplicates)
    output.json       → New translations from current batch (append-only)
----------------------------------------------------------------------------------------------------------------------

Basic Usage: 

Dry Run (Test Without Submitting)
python translate_batch.py input.json in_progress.json output.json text --dry-run

Test with 3 Articles
python translate_batch.py input.json in_progress.json output.json text --test 3

Submit Full Batch
# submit and exit (doesn't wait for completion)
python translate_batch.py input.json in_progress.json output.json text --submit-only

Process Results Later
# when batch completes (check status first)
python translate_batch.py input.json in_progress.json output.json text --process-only m

------------------------------------------------------------------------------------------
What the Script Does

    Reads input.json for Korean articles
    Checks in_progress.json and output.json to skip already-translated articles
    Creates batch requests for untranslated articles only
    Submits to Anthropic's Batch API
    Saves batch metadata to batch_info_{batch_id}.json and articles to batch_articles_{timestamp}.json
    Downloads results when batch completes
    Appends new translations to output.json (never modifies in_progress.json)

"""
import anthropic
import json
import argparse
import os
import time
import glob  
from datetime import datetime

# configuration
MODEL = "claude-sonnet-4-20250514"

os.environ['ANTHROPIC_API_KEY'] ="##########"

SYSTEM_PROMPT = """You are a professional Korean-English translator specializing in Korean education journalism. You are translating news articles about hagwon (private academy) teachers for qualitative content analysis by English-speaking researchers who are unfamiliar with Korean context.

## CRITICAL TERMINOLOGY RULES

You MUST use these exact translations consistently:

| Korean | English | Notes |
|--------|---------|-------|
| 학원 (hakwon) | hagwon | NEVER translate as "academy" or "cram school" |
| 어학원 | language hagwon | NEVER translate as "language academy" or "language institute" |
| 학원 강사 / 학원 교사 | hagwon instructor / hagwon teacher | Use interchangeably; Korean always specifies 학원 for hagwon teachers |
| 교사 / 교원 / 선생님 (without 학원) | school teacher / public school teacher | When used alone without 학원, these terms refer to school teachers by default |
| 사교육 | hagwon education | In Korean context, 사교육 refers specifically to the hagwon/tutoring industry; do NOT translate as "private education" |
| 공교육 | public education | |
| 전교조 | KTU (Korean Teachers and Education Workers Union, a left-leaning teachers' organization) | Provide full explanation EVERY time |
| 수능 / 대학수학능력시험 | CSAT (College Scholastic Ability Test, Korea's national university entrance exam) | Provide full explanation EVERY time |
| 내신 | school records / GPA | High school transcript grades |
| 입시 | college entrance exam(s) / university admissions | |
| 논술 | essay-writing exam | A component of some university admissions |
| 과외 | private tutoring | One-on-one tutoring, distinct from hagwon group instruction |
| EBS | EBS (Educational Broadcasting System, Korea's state-run educational media) | Provide explanation EVERY time |
| 강남 | Gangnam [an affluent district in Seoul] | Provide explanation EVERY time |
| 대치동 | Daechi-dong [a Gangnam neighborhood known as the epicenter of Korea's elite hagwon industry] | Provide explanation EVERY time |
| 스타강사 | star instructor / celebrity instructor | |
| 족집게 강사 | pinpoint instructor [a teacher known for accurately predicting exam questions] | Provide explanation |
| 재수생 | repeat test-taker [a student retaking CSAT after high school graduation] | Provide explanation |
| 기숙학원 | boarding hagwon | Residential hagwon where students live on-site |
| 수행평가 | performance assessment | School-based assessment (not standardized exam) |

## DISTINGUISHING HAGWON TEACHERS FROM SCHOOL TEACHERS

This is CRITICAL:
- 교사 (without specification) = school teacher. When Korean text uses 교사, 교원, or 선생님 alone, it refers to school teachers by default.
- 학원 강사 or 학원 교사 = hagwon teacher. Korean always specifies 학원 when referring to hagwon teachers.
- EXCEPTION FOR HAGWON CONTEXT: When the article establishes someone works at a hagwon (e.g., "어학원 강사" or "학원에서 일하는"), subsequent references like "영어강사" or "수학강사" should be translated as "English hagwon instructor" or "math hagwon instructor" — include "hagwon" to maintain clarity for English readers even if the Korean uses shorthand.
- For articles NOT about hagwon teachers, do not add "hagwon" where the Korean does not specify it.

## ANNOTATION RULES

Provide contextual annotations in square brackets for:

1. **Organization names** - Provide full name and brief explanation EVERY time (e.g., "KTU (Korean Teachers and Education Workers Union, a left-leaning teachers' organization)")

2. **Geographic references with social connotations** - Provide explanation EVERY time (e.g., "Gangnam [an affluent district in Seoul]" or "Daechi-dong [a Gangnam neighborhood known as the epicenter of Korea's elite hagwon industry]")

3. **Currency amounts** - ALWAYS provide approximate USD equivalent for ALL amounts, including vague amounts. Examples:
   - Specific: "50,000 won [approximately $38 USD]" or "2억원 [approximately $150,000 USD]"
   - Vague: "수백만원" → "millions of won [approximately $2,000–8,000 USD]" or "수천만원" → "tens of millions of won [approximately $15,000–75,000 USD]"

4. **Historical or political references** unfamiliar to international readers (e.g., "386 generation [Koreans born in the 1960s who were politically active in the 1980s democracy movement]")

5. **Educational system-specific terms** not in the terminology list above

## TRANSLATION PRINCIPLES

1. **Preserve framing and tone**: Your primary goal is to preserve HOW hagwon teachers are portrayed—positively, negatively, or neutrally. Capture connotations even if it requires non-literal translation.

2. **Preserve evaluative language**: Maintain loaded verbs, intensifiers, hedges, and scare quotes. Do not soften or amplify the original tone.

3. **Do not neutralize or editorialize**: Transfer the original tone faithfully, not improve or correct it.

4. **Names**: Romanize Korean names using Revised Romanization. Preserve anonymization format (e.g., "Mr. Kim (30)" or "A (female, 25)").

5. **Quotations**: Preserve all direct quotations with proper attribution.

## OUTPUT FORMAT

Return ONLY a JSON object with two fields:
{
  "translated_title": "[translated title here]",
  "translated_text": "[translated body text here with all annotations]"
}

Do not include any explanation or commentary outside the JSON object."""

# token costs per million tokens (batch is 50% cheaper!)
INPUT_TOKEN_COST = 1.5
OUTPUT_TOKEN_COST = 7.5

# === FUNCTIONS ===

def load_translated_urls(in_progress_file, output_file):
    """load urls from both in_progress and output (don't modify either)"""
    translated_urls = set()
    
    # check in_progress (existing translations - read-only)
    if os.path.exists(in_progress_file):
        with open(in_progress_file, 'r', encoding='utf-8') as f:
            in_progress_articles = json.load(f)
        in_progress_urls = {a['url'] for a in in_progress_articles}
        translated_urls.update(in_progress_urls)
        print(f"found {len(in_progress_urls)} in in_progress (read-only)")
    
    # check output (previous batch outputs)
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            output_articles = json.load(f)
        output_urls = {a['url'] for a in output_articles}
        translated_urls.update(output_urls)
        print(f"found {len(output_urls)} in output")
    
    print(f"total already translated: {len(translated_urls)}")
    
    return translated_urls

def create_batch_requests(articles, text_field, translated_urls, limit=None):
    """create batch api requests for untranslated articles"""
    
    # filter to only untranslated articles with content
    articles_to_translate = []
    for article in articles:
        url = article.get('url', '')
        text = article.get(text_field, '')
        
        if url not in translated_urls and text:
            articles_to_translate.append(article)
    
    print(f"filtered to {len(articles_to_translate)} untranslated articles")
    
    # apply limit if testing
    if limit:
        articles_to_translate = articles_to_translate[:limit]
        print(f"limiting to {limit} articles for test")
    
    # create requests using consistent indices
    requests = []
    for i, article in enumerate(articles_to_translate):
        url = article.get('url', '')
        title = article.get('title', '')
        text = article.get(text_field, '')
        
        user_message = f"""Translate the following Korean news article into English following all the guidelines in your instructions.

TITLE: {title}

BODY:
{text}

Return ONLY the JSON object with translated_title and translated_text."""
        
        # batch request format
        request = {
            "custom_id": f"article_{i}",
            "params": {
                "model": MODEL,
                "max_tokens": 12000,
                "system": SYSTEM_PROMPT,
                "messages": [
                    {"role": "user", "content": user_message}
                ]
            }
        }
        
        requests.append(request)
    
    return requests, articles_to_translate

def save_batch_requests(requests, output_file):
    """save batch requests to jsonl file"""
    with open(output_file, 'w', encoding='utf-8') as f:
        for request in requests:
            f.write(json.dumps(request, ensure_ascii=False) + '\n')
    
    print(f"✓ saved {len(requests)} batch requests to: {output_file}")

def submit_batch(client, requests_file):
    """submit batch job to anthropic"""
    print("\nsubmitting batch job...")
    
    try:
        # load requests as list of dicts
        requests_list = []
        with open(requests_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    requests_list.append(json.loads(line))
        
        print(f"loaded {len(requests_list)} requests from file")
        
        # submit batch
        batch = client.messages.batches.create(
            requests=requests_list
        )
        
        print(f"✓ batch submitted successfully!")
        print(f"  batch id: {batch.id}")
        print(f"  status: {batch.processing_status}")
        print(f"  request counts: {batch.request_counts}")
        
        return batch
        
    except anthropic.APIError as e:
        print(f"\n✗ api error: {e}")
        raise
    except Exception as e:
        print(f"\n✗ unexpected error: {e}")
        raise

def poll_batch_status(client, batch_id, poll_interval=60):
    """poll batch status until complete"""
    print(f"\npolling batch status (checking every {poll_interval}s)...")
    print("press ctrl+c to stop polling (batch will continue processing)")
    
    try:
        while True:
            batch = client.messages.batches.retrieve(batch_id)
            
            status = batch.processing_status
            counts = batch.request_counts
            
            # access pydantic attributes directly
            total = counts.processing + counts.succeeded + counts.errored
            processed = counts.succeeded + counts.errored
            
            print(f"\rstatus: {status} | processed: {processed}/{total} | succeeded: {counts.succeeded} | errored: {counts.errored}", end='', flush=True)
            
            if status == "ended":
                print("\n✓ batch processing complete!")
                print(f"  succeeded: {counts.succeeded}")
                print(f"  errored: {counts.errored}")
                print(f"  canceled: {counts.canceled}")
                print(f"  expired: {counts.expired}")
                return batch
            
            elif status in ["canceling", "canceled"]:
                print(f"\n✗ batch was canceled")
                return batch
            
            elif status == "expired":
                print(f"\n✗ batch expired")
                return batch
            
            time.sleep(poll_interval)
            
    except KeyboardInterrupt:
        print(f"\n\n⚠ polling interrupted by user")
        print(f"batch is still processing in background")
        print(f"batch id: {batch_id}")
        return None

def download_results(client, batch, output_file):
    """download batch results"""
    print(f"\ndownloading results...")
    
    if not batch.results_url:
        print("✗ no results url available")
        return None
    
    try:
        results_response = client.messages.batches.results(batch.id)
        
        # save results to file
        with open(output_file, 'w', encoding='utf-8') as f:
            for result in results_response:
                f.write(json.dumps(result.model_dump(), ensure_ascii=False) + '\n')
        
        print(f"✓ results saved to: {output_file}")
        return output_file
        
    except Exception as e:
        print(f"✗ error downloading results: {e}")
        raise

def process_batch_results(results_file, batch_articles_file, text_field):
    """process batch results using batch_articles file for matching"""
    print("\nprocessing batch results...")
    
    # load THIS batch's articles (in exact order)
    with open(batch_articles_file, 'r', encoding='utf-8') as f:
        articles_submitted = json.load(f)
    
    print(f"loaded {len(articles_submitted)} articles from batch")
    
    # load results
    results = []
    with open(results_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                results.append(json.loads(line))
    
    print(f"loaded {len(results)} results")
    
    # create mapping from custom_id to result
    results_map = {}
    for result in results:
        custom_id = result.get('custom_id', '')
        results_map[custom_id] = result
    
    # process results
    translated_articles = []
    successful = 0
    errors = []
    
    for i, article in enumerate(articles_submitted):
        url = article.get('url', '')
        custom_id = f"article_{i}"
        
        if custom_id not in results_map:
            print(f"  ⚠ no result for {custom_id}")
            continue
        
        result = results_map[custom_id]
        result_type = result.get('result', {}).get('type', 'unknown')
        
        if result_type == 'succeeded':
            try:
                # extract translation
                message = result['result']['message']
                content = message['content'][0]['text']
                
                # handle markdown
                if content.startswith("```"):
                    content = content.split("```")[1]
                    if content.startswith("json"):
                        content = content[4:]
                    content = content.strip()
                
                # parse json
                translation = json.loads(content)
                
                # create translated article in correct format
                translated_article = {
                    'url': url,
                    'date': article.get('date', article.get('datetime', '')),
                    'publication': article.get('publication', ''),
                    'original_title': article.get('title', ''),
                    'translated_title': translation.get('translated_title', ''),
                    'original_text': article.get(text_field, ''),
                    'translated_text': translation.get('translated_text', ''),
                    'include': article.get('include', True),
                    'reason': article.get('reason', '')
                }
                
                translated_articles.append(translated_article)
                successful += 1
                
            except Exception as e:
                print(f"  ✗ processing error: {custom_id} - {e}")
                errors.append({
                    'custom_id': custom_id,
                    'url': url,
                    'error': str(e),
                    'type': 'processing_error',
                    'original_title': article.get('title', '')[:100]
                })
                
        elif result_type == 'errored':
            # extract error details
            error_info = result.get('result', {}).get('error', {})
            error_type = error_info.get('type', 'unknown_error')
            error_msg = error_info.get('message', 'no error message')
            
            errors.append({
                'custom_id': custom_id,
                'url': url,
                'error': error_msg,
                'type': error_type,
                'original_title': article.get('title', '')[:100]
            })
    
    print(f"✓ processed {successful} successful translations")
    
    if errors:
        print(f"✗ {len(errors)} errors")
        
        # save error log
        error_log_file = f"batch_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(error_log_file, 'w', encoding='utf-8') as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
        print(f"  📄 error details: {error_log_file}")
    
    return translated_articles

def dry_run_batch(requests, articles_to_translate):
    """show what would be submitted"""
    print("\n" + "=" * 60)
    print("🧪 dry run mode")
    print("=" * 60)
    
    print(f"\nwould submit {len(requests)} translation requests")
    
    # show sample
    for i, req in enumerate(requests[:3]):
        article = articles_to_translate[i]
        print(f"\n{i+1}. custom_id: {req['custom_id']}")
        print(f"   title: {article.get('title', '')[:60]}...")
        print(f"   url: {article.get('url', '')}")
    
    if len(requests) > 3:
        print(f"\n... and {len(requests) - 3} more")
    
    # estimate cost
    avg_input = 1500
    avg_output = 3000
    total_input = len(requests) * avg_input
    total_output = len(requests) * avg_output
    cost = (total_input * INPUT_TOKEN_COST + total_output * OUTPUT_TOKEN_COST) / 1_000_000
    
    print(f"\nestimated cost: ${cost:.2f}")
    print(f"processing time: 1-24 hours")
    
    print("\n✓ dry run complete - nothing submitted")

def main():
    parser = argparse.ArgumentParser(
        description='translate korean articles using batch api (50% cheaper!)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
three-file system:
  input.json       - all korean articles (never modified)
  in_progress.json - already translated (read-only, checked for duplicates)
  output.json      - new translations from this batch (write-only)

examples:
  # dry run
  python translate_batch.py input.json in_progress.json output.json text --dry-run
  
  # test 3 articles
  python translate_batch.py input.json in_progress.json output.json text --test 3
  
  # submit full batch
  python translate_batch.py input.json in_progress.json output.json text --submit-only
  
  # process later
  python translate_batch.py input.json in_progress.json output.json text --process-only batch_abc123
        """
    )
    parser.add_argument('input_file', help='input json (korean articles)')
    parser.add_argument('in_progress_file', help='in-progress json (already translated - read-only)')
    parser.add_argument('output_file', help='output json (new translations)')
    parser.add_argument('text_field', help='field name for article text')
    parser.add_argument('-k', '--api-key', help='anthropic api key')
    parser.add_argument('--submit-only', action='store_true', help='submit and exit')
    parser.add_argument('--process-only', help='process batch (provide batch_id)')
    parser.add_argument('--poll-interval', type=int, default=300, help='poll interval (seconds)')
    parser.add_argument('--test', type=int, metavar='N', help='test with N articles')
    parser.add_argument('--dry-run', action='store_true', help='dry run (no api calls)')
    
    args = parser.parse_args()
    
    # get api key
    api_key = args.api_key or os.environ.get('ANTHROPIC_API_KEY')
    if not api_key and not args.dry_run:
        print("❌ api key required")
        return
    
    client = None if args.dry_run else anthropic.Anthropic(api_key=api_key)
    
    print("\n" + "=" * 80)
    print("translate korean articles - batch api (50% savings!)")
    print("=" * 80)
    print(f"input:       {args.input_file} (korean articles)")
    print(f"in-progress: {args.in_progress_file} (already translated - read-only)")
    print(f"output:      {args.output_file} (new translations)")
    
    # === MODE 1: PROCESS EXISTING BATCH ===
    if args.process_only:
        print(f"\nmode: processing batch {args.process_only}")
        
        # load batch info
        batch_info_file = f"batch_info_{args.process_only}.json"
        if not os.path.exists(batch_info_file):
            print(f"✗ batch info file not found")
            return
        
        with open(batch_info_file, 'r', encoding='utf-8') as f:
            batch_info = json.load(f)
        
        # load THIS batch's articles from batch_articles file
        batch_articles_file = batch_info['batch_articles_file']
        if not os.path.exists(batch_articles_file):
            print(f"✗ batch articles file not found: {batch_articles_file}")
            return
        
        with open(batch_articles_file, 'r', encoding='utf-8') as f:
            articles_submitted = json.load(f)
        
        print(f"batch contained {len(articles_submitted)} articles")
        
        # download results
        results_file = f"batch_results_{args.process_only}.jsonl"
        
        try:
            batch = client.messages.batches.retrieve(args.process_only)
            
            if batch.processing_status != "ended":
                print(f"\n⚠ batch status: {batch.processing_status}")
                print(f"  counts: {batch.request_counts}")
                return
            
            download_results(client, batch, results_file)
            
        except anthropic.NotFoundError:
            print(f"✗ batch not found: {args.process_only}")
            return
        except Exception as e:
            print(f"✗ error: {e}")
            return
        
        # process results using batch_articles file
        translated_articles = process_batch_results(
            results_file,
            batch_articles_file,
            args.text_field
        )
        
        # load existing output and append
        existing_output = []
        if os.path.exists(args.output_file):
            with open(args.output_file, 'r', encoding='utf-8') as f:
                existing_output = json.load(f)
            print(f"\nloaded {len(existing_output)} existing from output")
        
        all_translations = existing_output + translated_articles
        
        # save to output
        with open(args.output_file, 'w', encoding='utf-8') as f:
            json.dump(all_translations, f, ensure_ascii=False, indent=2)
        
        print(f"\n✓ appended {len(translated_articles)} new translations")
        print(f"📊 total in output: {len(all_translations)}")
        print(f"📁 saved to: {args.output_file}")
        print(f"\n💡 note: in_progress.json was NOT modified (read-only)")
        return
    
    # === MODE 2: SUBMIT NEW BATCH ===
    
    print("\nstep 1: loading input articles")
    with open(args.input_file, 'r', encoding='utf-8') as f:
        input_articles = json.load(f)
    
    print(f"loaded {len(input_articles)} articles from input")
    
    # check BOTH in_progress AND output for already-translated urls
    translated_urls = load_translated_urls(args.in_progress_file, args.output_file)
    
    # calculate what needs translation
    need_translation = sum(1 for a in input_articles if a.get('url') not in translated_urls)
    print(f"need translation: {need_translation}")
    
    if need_translation == 0:
        print("\n✓ all articles already translated!")
        return
    
    # create batch requests
    print("\nstep 2: creating batch requests")
    
    limit = args.test if args.test else None
    if args.test:
        print(f"🧪 test mode: {args.test} articles")
    
    requests, articles_to_translate = create_batch_requests(
        input_articles,
        args.text_field,
        translated_urls,
        limit=limit
    )
    
    if not requests:
        print("✗ no articles to translate")
        return
    
    print(f"created {len(requests)} requests")
    
    # dry run
    if args.dry_run:
        dry_run_batch(requests, articles_to_translate)
        return
    
    # save requests
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    requests_file = f"batch_requests_{timestamp}.jsonl"
    save_batch_requests(requests, requests_file)
    
    # save THIS BATCH's articles to timestamped file (NOT in_progress!)
    batch_articles_file = f"batch_articles_{timestamp}.json"
    with open(batch_articles_file, 'w', encoding='utf-8') as f:
        json.dump(articles_to_translate, f, ensure_ascii=False, indent=2)
    print(f"✓ saved batch articles to: {batch_articles_file}")
    print(f"💡 in_progress.json NOT modified (read-only)")
    
    # submit
    print("\nstep 3: submitting batch")
    try:
        batch = submit_batch(client, requests_file)
    except Exception:
        print("\nsubmission failed")
        return
    
    # save batch info
    batch_info = {
        'batch_id': batch.id,
        'submitted_at': timestamp,
        'input_file': args.input_file,
        'in_progress_file': args.in_progress_file,
        'batch_articles_file': batch_articles_file,
        'output_file': args.output_file,
        'requests_file': requests_file,
        'num_requests': len(requests)
    }
    
    batch_info_file = f"batch_info_{batch.id}.json"
    with open(batch_info_file, 'w', encoding='utf-8') as f:
        json.dump(batch_info, f, indent=2)
    
    print(f"✓ batch info: {batch_info_file}")
    
    if args.submit_only:
        print("\n" + "=" * 80)
        print("batch submitted!")
        print("=" * 80)
        print(f"\nprocess later:")
        print(f"  python translate_batch.py {args.input_file} {args.in_progress_file} {args.output_file} {args.text_field} --process-only {batch.id}")
        return
    
    # wait
    print("\nstep 4: waiting")
    batch = poll_batch_status(client, batch.id, args.poll_interval)
    
    if not batch:
        return
    
    # download
    print("\nstep 5: downloading")
    results_file = f"batch_results_{batch.id}.jsonl"
    download_results(client, batch, results_file)
    
    # process
    print("\nstep 6: processing")
    translated_articles = process_batch_results(
        results_file,
        batch_articles_file,
        args.text_field
    )
    
    # load existing output and append
    existing = []
    if os.path.exists(args.output_file):
        with open(args.output_file, 'r', encoding='utf-8') as f:
            existing = json.load(f)
        print(f"loaded {len(existing)} existing from output")
    
    all_translations = existing + translated_articles
    
    # save to output
    with open(args.output_file, 'w', encoding='utf-8') as f:
        json.dump(all_translations, f, ensure_ascii=False, indent=2)
    
    print(f"\n✓ appended {len(translated_articles)} translations")
    print(f"📊 total in output: {len(all_translations)}")
    print(f"📁 saved to: {args.output_file}")

if __name__ == "__main__":
    main()