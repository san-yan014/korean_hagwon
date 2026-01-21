"""
This script classifies korean newspaper articles about hagwon instructors using claude's api. It analyzes how articles portray hagwon teachers
across 16 stigmatization categories. 

input:
    json file with these columns:

    article_url - unique identifier
    date - publication date
    translated_text - english translation of article
    publication - article's publication

output: 
    
article_id	date	code	code_5_sub	justification	key_quote
8183418	2005-04-26	5	a, d	Arrest for sexual assault and robbery.	"Song (36), an instructor..."
8183418	2005-04-26	15		High earner framing.	"earns 10 million won per month"

example: 

    # estimate only
    python script.py articles.json --estimate-only

    # run standard api
    python script.py articles.json --output results.csv

    # batch api
    python script.py articles.json --batch --submit-batch

    # process batch 
    python script.py articles.json --process-batch msgbatch_YOUR_BATCH_ID --output results.csv
"""
import anthropic
import csv
import json
from pathlib import Path
import tiktoken
import os

os.environ['ANTHROPIC_API_KEY'] ="#################"

# pricing per million tokens
SONNET_INPUT_COST = 3.00  # $3 per million input tokens
SONNET_OUTPUT_COST = 15.00  # $15 per million output tokens
BATCH_DISCOUNT = 0.5  # 50% discount for batch api


# codebook definition for the prompt
CODEBOOK = """
You are analyzing korean newspaper articles about hagwon (private academy) instructors. classify each article according to the following codebook:

INTERPRETATION PRINCIPLES:
- articles may have multiple codes - assign all that clearly apply based on substantial textual evidence
- for code 5, always specify subcategory (a, b, c, or d)
- focus on the article's framing and tone toward hagwon instructors, not just factual content
- use inclusion/exclusion criteria as guides, not absolute rules - consider the broader context of how the article portrays hagwon instructors
- when uncertain between codes, consider: what is the article's primary message about these instructors?

CODE 1: FACILITATOR OF ACADEMIC DISHONESTY
definition: the article portrays hagwon teachers as helping students cheat, violate academic ethics, or engage in academic fraud.
inclusion: explicit description of completing homework/essays for students, leaking exam questions, helping fabricate research/competition entries, producing work to be submitted as student's own.
exclusion: general criticism without specific dishonesty reference, legitimate tutoring/exam prep, parents (not teachers) facilitating cheating.
example: "then, in early april, when the contest theme was announced... kim and a pre-arranged hagwon teacher began writing the report."

CODE 2: CONDUIT OF SOCIAL INEQUITY
definition: the article portrays hagwon teachers as agents who reproduce or amplify socioeconomic inequality by providing advantages exclusively to families who can afford to pay.
inclusion: framing hagwon education as creating unfair advantages for wealthy families, discussion of widening gaps between rich and poor students, criticism of for-profit nature as inherently inequitable.
exclusion: neutral pricing descriptions, affordability discussions without equity framing, criticism on other grounds.
key test: does the article frame the teacher's work as contributing to social stratification because it is fee-for-service?

CODE 3: PROFIT MOTIVE DOUBTED
definition: the article casts doubt on hagwon teachers' commitment to genuine educational goals, suggesting primary motivation is financial gain rather than student welfare.
inclusion: questioning whether teachers care about education or only money, using public platforms primarily to recruit paying clients, conflicts of interest where profit compromises integrity, prioritizing revenue over student outcomes.
exclusion: neutral income mentions, high-earning descriptions without questioning motives.
example: "the problem is that the agents of this reform are private education instructors whose goal is profit... they had no reason to refuse ebs's offer because it provides an incentive to easily attract students."

CODE 4: OPPOSED AS UNDERMINING PUBLIC EDUCATION
definition: the article portrays hagwon teachers or industry as targets of opposition on grounds that they undermine public education.
inclusion: any group opposing hagwons for undermining public education, protests/complaints/legal actions framing hagwons as harmful to public schooling, quotes criticizing for weakening/competing with/replacing public education, self-criticism from within hagwon industr, characterizations of hagwon education as 'evil' or problematic for public education. 
exclusion: criticism on other grounds (crime, dishonesty, profit), opposition based on cost/inequity without public education reference.
example: "the korean teachers and education workers union... filed a complaint... claiming that 'the local government is promoting hagwon education service.'"

CODE 5: CRIMINAL CONDUCT
definition: the article portrays hagwon teachers as perpetrators of criminal offenses.
subcategories: (a) sexual misconduct with students, (b) sexual misconduct with non-students, (c) drug-related crimes, (d) violent crimes or financial fraud.
inclusion: description of sexual offenses, drug use/possession/distribution, violent crimes, financial fraud, illegal tutoring operations.
exclusion: false allegations (code 8), civil disputes, ethical violations that aren't criminal.
examples: 
- (a) "choi (26), an english hagwon teacher indicted on charges of sexually assaulting three kindergarten students."
- (c) "kim purchased marijuana from an english hagwon and smoked it...18 times."

CODE 6: UNQUALIFIED / SUBSTANDARD CREDENTIALS
definition: the article portrays hagwon teachers as lacking proper qualifications, operating without licenses, or working in unregulated settings.
inclusion: mention of unlicensed hagwons with unverified instructors, teachers without proper credentials/training, regulatory failures allowing unqualified individuals, framing as less credentialed than public school teachers.
exclusion: criticism of teaching quality without credential reference, pursuing continuing education, neutral background mentions.
example: "unlicensed boarding hagwons are more likely to offer unverified instructors, uncomfortable accommodations, and substandard meals."

CODE 7: PRECARIOUS / MARGINAL EMPLOYMENT
definition: the article portrays hagwon teaching as economically insecure, low-paid, or characterized by labor-market vulnerability.
inclusion: listing alongside freelancers/daily workers/precarious categories, reports of low wages/wage theft/minimum wage disputes, financial struggles/hardship, policy discussions framing as non-standard workers requiring protection.
exclusion: moderate/high earnings descriptions, stepping stone/transitional job references, general industry descriptions.
example: "for the first-time homebuyer special supply, subscription eligibility will also be given to those who do not pay earned income tax, such as insurance planners, hagwon instructors, daily workers, and freelancers."

CODE 8: VICTIM OF DEFAMATION / FALSE ACCUSATIONS
definition: the article portrays a hagwon teacher as having been wrongly accused, defamed, or having reputation unfairly damaged by false claims.
inclusion: reporting accusations proven false, descriptions as victims of malicious rumors/defamation, legal cases where teachers are plaintiffs, narratives emphasizing injustice through false claims.
exclusion: credible or proven accusations, general criticism, ongoing investigations without resolution.

CODE 9: FALLBACK CAREER FOR THE EDUCATED
definition: the article portrays hagwon teaching as a default or last-resort occupation for educated individuals who failed to secure more desirable employment.
inclusion: turning to hagwon teaching after failing to find preferred jobs, framing as what educated people do when options exhausted, job search failure followed by hagwon entry, references as "low-cost labor" from educated unemployed.
exclusion: temporary/transitional by choice, neutral career path descriptions, successful/high-earning teacher descriptions.
key distinction from code 10: fallback implies failure; transitional implies temporary by design.
example: "kim young-sun (25, female)... applied to 19 internships and 26 new employee open recruitments... was rejected from all... these days, she has given up on joining a large corporation and is looking for a position as a hagwon instructor."

CODE 10: TRANSITIONAL OR PART-TIME WORK
definition: the article portrays hagwon teaching as temporary employment while pursuing other goals or as casual part-time work by students.
inclusion: doing hagwon teaching while completing advanced degrees, university students working part-time, framing as temporary job during life stage, using hagwon teaching to support other pursuits.
exclusion: last resort after career failure, long-term career teachers, primary chosen profession.
key distinction from code 9: transitional implies temporary by choice; fallback implies lack of options.
example: "after returning to korea, he worked as a hagwon instructor in seoul while completing his doctoral program."

CODE 11: SUGGESTED AFFORDABILITY
definition: the article portrays hagwon teachers' services as accessible, affordable, or moderately priced, positioning them in mass-market rather than elite tier.
inclusion: tuition fees in low-to-moderate ranges, framing as affordable alternatives, price competition/cost-effectiveness references, policy discussions about "low-cost" supplementary education.
exclusion: high-cost/elite services, pricing in inequity context, star teachers' premium fees.
example: "mimacstudy has opened a total of 79 courses... tuition fees range from 10,000 to 80,000 won [approximately $8–$62 usd]."

CODE 12: EDUCATIONALLY COUNTERPRODUCTIVE
definition: the article portrays hagwon education as producing outcomes contrary to genuine learning, undermining educational goals, or causing harm to students' wellbeing.
inclusion: framing as ineffective/counterproductive to real learning, methods that undermine intended outcomes, excessive academic burden/psychological harm, parents' investments producing opposite results.
exclusion: ethical criticism without efficacy claims, general industry critiques, competition/market dynamics without learning-outcome framing.

CODE 13: FUNCTIONAL EDUCATIONAL SERVICE
definition: the article portrays hagwon teachers in a neutral, descriptive manner as providers of standard educational services.
inclusion: neutral descriptions of delivering instruction, factual reporting on services/schedules/curriculum, mentions in educational contexts without positive/negative framing.
exclusion: evaluative portrayals, emphasis on expertise/recognition, emphasis on problems.
note: most neutral code; use when mentioned in professional capacity without clear positive/negative framing.

CODE 14: RECOGNIZED EXPERT / PROFESSIONAL PEER
definition: the article portrays hagwon teachers as legitimate professionals with expertise, credibility, and standing comparable to other educators.
inclusion: hagwon instructors quoted as expert commentators on educational policy or pedagogy, hagwon instructors recruited for public education roles based on their expertise, hagwon instructors framed as comparable to or competitive with public school teachers in terms of professional standing, hagwon instructors described as trusted/effective educational guides, hagwon instructors grouped with high-status professionals in regulatory contexts, pursuing continuing education to enhance expertise.
exclusion: instructors who are not hagwon instructors (e.g., public school teachers, university professors), neutral descriptions without expertise framing, emphasizing financial success, critical portrayals of qualifications.
example: "one college entrance exam hagwon instructor pointed out, 'in our educational environment... performance-based assessment is a system that leans too much toward idealism.'"

CODE 15: GLAMOROUS / HIGH-EARNING WORKER
definition: the article portrays hagwon teachers as exceptionally successful, high-earning, or celebrity-like figures.
inclusion: earning salaries in billions of won, celebrity instructors with public recognition, recruited by media companies, framed as path to exceptional wealth/fame.
exclusion: typical/low earnings descriptions, moderate pricing, professional recognition without financial emphasis.
example: "online hagwon instructors who are already earning annual salaries in the tens of billions of won."

CODE 16: ORDINARY CITIZEN
definition: the article mentions a hagwon teacher incidentally as a relatable, everyday member of society.
inclusion: mentioned as example of ordinary consumer/citizen in non-education news, quoted discussing topics unrelated to hagwon education, engaged in community activities/volunteering/civic life, used as relatable "person on the street" example, obituaries/personal profiles in everyday contexts.
exclusion: articles focused on professional roles, mentions in educational policy contexts, descriptions emphasizing occupational characteristics.
example: "kim hyo-jung (31, a hagwon instructor), whom i met at the store... said, 'when i buy clothes at adult stores, sometimes the alteration costs end up being more than the price of the clothes themselves.'"

"""

def estimate_tokens(text):
    """estimate token count using tiktoken"""
    enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))

def estimate_cost(input_file, use_batch=False):
    """estimate api costs before running"""
    with open(input_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    # estimate tokens per article
    system_tokens = estimate_tokens(CODEBOOK)
    total_input_tokens = 0
    total_output_tokens = 0
    
    for article in articles:
        article_text = article.get('translated_text', '')
        title =  article.get('translated_title', '')
        date = article.get('date', '')
        
        prompt_tokens = estimate_tokens(f"title: {title}\ndate: {date}\n\n{article_text}")
        total_input_tokens += system_tokens + prompt_tokens
        
        # estimate ~500 tokens output per article (conservative)
        total_output_tokens += 500
    
    # convert to millions
    input_millions = total_input_tokens / 1_000_000
    output_millions = total_output_tokens / 1_000_000
    
    # calculate costs
    discount_multiplier = BATCH_DISCOUNT if use_batch else 1.0
    input_cost = input_millions * SONNET_INPUT_COST * discount_multiplier
    output_cost = output_millions * SONNET_OUTPUT_COST * discount_multiplier
    total_cost = input_cost + output_cost
    
    print(f"\n{'='*60}")
    print(f"cost estimate for {len(articles)} articles")
    print(f"{'='*60}")
    print(f"estimated input tokens: {total_input_tokens:,}")
    print(f"estimated output tokens: {total_output_tokens:,}")
    print(f"api mode: {'batch (50% discount)' if use_batch else 'standard'}")
    print(f"\ninput cost: ${input_cost:.2f}")
    print(f"output cost: ${output_cost:.2f}")
    print(f"total estimated cost: ${total_cost:.2f}")
    print(f"{'='*60}\n")
    
    return total_cost, len(articles)

def create_classification_prompt(article_text, title, date):
    """create the prompt for claude to classify the article"""
    return f"""analyze this korean newspaper article about hagwon instructors and classify it according to the provided codebook.

title: {title}
date: {date}

article text:
{article_text}

instructions:
1. identify ALL applicable codes (1-16) from the codebook
2. for code 5 (criminal conduct), specify subcategories: a, b, c, or d
3. for each code, provide a brief justification
4. extract a key quote from the article that supports each code
5. if the article contains NO stigmatizing content about hagwon instructors, return an empty array: []

important guidelines:
- articles often contain multiple forms of stigmatization - assign all codes that clearly apply
- be thorough but not excessive - assign codes only where evidence is substantial


return your analysis as a json array where each element represents one code assignment:
[
  {{
    "code": 5,
    "code_5_sub": "a,d",
    "justification": "brief explanation",
    "key_quote": "relevant quote from article"
  }},
  {{
    "code": 15,
    "code_5_sub": null,
    "justification": "brief explanation",
    "key_quote": "relevant quote from article"
  }}
]

return ONLY the json array, no other text."""

def classify_articles_standard(input_file, output_file):
    """classify articles using standard api with streaming"""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(
        api_key=api_key,
        timeout=600.0
    )
    
    with open(input_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    results = []
    
    for i, article in enumerate(articles, 1):
        title = article.get('translated_title', '')
        date = article.get('date', '')
        url = article.get('url', '')  # get url
        publication = article.get('publication', '')
        article_text = article.get('translated_text', '')
        
        # check token count
        prompt_tokens = estimate_tokens(create_classification_prompt(article_text, title, date))
        system_tokens = estimate_tokens(CODEBOOK)
        total_tokens = prompt_tokens + system_tokens
        
        print(f"processing article {i}/{len(articles)}: {url}")  # print url
        print(f"  tokens: {total_tokens:,} (system: {system_tokens:,}, prompt: {prompt_tokens:,})")
        
        if total_tokens > 180000:
            print(f"  warning: very long input, may be slow")
        
        try:
            # use streaming to avoid timeout
            response_text = ""
            with client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=8192,
                system=CODEBOOK,
                messages=[{
                    "role": "user",
                    "content": create_classification_prompt(article_text, title, date)
                }],
                timeout=600.0
            ) as stream:
                for text in stream.text_stream:
                    response_text += text
            
            # remove markdown code blocks if present
            response_text = response_text.strip()
            if response_text.startswith("```"):
                response_text = response_text.split('\n', 1)[1] if '\n' in response_text else response_text[3:]
                if response_text.endswith("```"):
                    response_text = response_text[:-3]
                response_text = response_text.strip()
            
            # try to parse json
            try:
                codes = json.loads(response_text)
            except json.JSONDecodeError as je:
                print(f"  json parse error")
                print(f"  response preview: {response_text[:200]}...")
                with open(f"error_response_{i}.txt", 'w', encoding='utf-8') as ef:
                    ef.write(response_text)
                continue
            
            # handle empty results
            if not codes or not isinstance(codes, list):
                print(f"  warning: no codes returned")
                continue
            
            print(f"  success: {len(codes)} codes assigned")
            
            # create one row per code
            for code_entry in codes:
                results.append({
                    'url': url,  # changed from title to url
                    'date': date,
                    'publication': publication,
                    'code': code_entry.get('code'),
                    'code_5_sub': code_entry.get('code_5_sub'),
                    'justification': code_entry.get('justification', ''),
                    'key_quote': code_entry.get('key_quote', '')
                })
                
        except Exception as e:
            print(f"  error: {e}")
            continue
    
    # write results to csv
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'date', 'publication', 'code', 'code_5_sub', 'justification', 'key_quote'])  # changed fieldnames
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nclassification complete! {len(results)} codes assigned from {len(articles)} articles")
    print(f"results saved to {output_file}")

def create_batch_requests(input_file, batch_file):
    """create batch api request file"""
    with open(input_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    requests = []
    # create a mapping file to track custom_id to url
    id_mapping = []
    
    for i, article in enumerate(articles):
        title = article.get('translated_title', '')
        date = article.get('date', '')
        url = article.get('url', '')
        article_text = article.get('translated_text', '')
        
        # create a sanitized custom_id
        custom_id = f"article_{i}"
        
        # store mapping for later
        id_mapping.append({
            'custom_id': custom_id,
            'url': url,
            'date': date,
            'publication': article.get('publication', '')
        })
        
        request = {
            "custom_id": custom_id,  # use sanitized id
            "params": {
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 15000,
                "system": CODEBOOK,
                "messages": [{
                    "role": "user",
                    "content": create_classification_prompt(article_text, title, date)
                }]
            }
        }
        requests.append(request)
    
    # write jsonl file
    with open(batch_file, 'w', encoding='utf-8') as f:
        for request in requests:
            f.write(json.dumps(request) + '\n')
    
    # save mapping file
    mapping_file = batch_file.replace('.jsonl', '_mapping.json')
    with open(mapping_file, 'w', encoding='utf-8') as f:
        json.dump(id_mapping, f, ensure_ascii=False, indent=2)
    
    print(f"batch request file created: {batch_file}")
    print(f"mapping file created: {mapping_file}")
    print(f"total requests: {len(requests)}")

def submit_batch(batch_file):
    """submit batch job to anthropic"""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    
    # read the jsonl file and parse into a list of requests
    requests = []
    with open(batch_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():  # skip empty lines
                requests.append(json.loads(line))
    
    # create batch with the list of requests
    batch = client.messages.batches.create(
        requests=requests
    )
    
    print(f"\nbatch submitted successfully!")
    print(f"batch id: {batch.id}")
    print(f"status: {batch.processing_status}")
    print(f"\ncheck status with: batch_id = '{batch.id}'")
    
    # save batch id to file
    with open('batch_id.txt', 'w') as f:
        f.write(batch.id)
    print(f"batch id saved to batch_id.txt")
    
    return batch.id

def process_batch_results(batch_id, input_file, output_file):
    """process completed batch results"""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=api_key)
    
    # get batch status
    batch = client.messages.batches.retrieve(batch_id)
    
    if batch.processing_status != "ended":
        print(f"batch not complete yet. status: {batch.processing_status}")
        return
    
    print(f"batch complete! processing results...")
    
    # load mapping file
    mapping_file = 'batch_requests_mapping.json'
    try:
        with open(mapping_file, 'r', encoding='utf-8') as f:
            id_mapping = json.load(f)
    except OSError as e:
        print(f"error reading mapping file: {e}")
        print(f"try recreating the mapping file with: python classify.py {input_file} --batch")
        return
    
    # create lookup by custom_id
    id_lookup = {item['custom_id']: item for item in id_mapping}
    
    # get results iterator
    results = []
    errors = []
    
    for result in client.messages.batches.results(batch_id):
        custom_id = result.custom_id
        article_info = id_lookup.get(custom_id)
        
        if not article_info:
            print(f"warning: no mapping found for {custom_id}")
            continue
        
        url = article_info['url']
        date = article_info['date']
        publication = article_info['publication']
        
        if result.result.type == "succeeded":
            response_text = result.result.message.content[0].text.strip()
            
            # save original response before any processing
            full_response = response_text
            
            # remove markdown code blocks if present
            if response_text.startswith("```"):
                response_text = response_text.split('\n', 1)[1] if '\n' in response_text else response_text[3:]
                if response_text.endswith("```"):
                    response_text = response_text[:-3]
                response_text = response_text.strip()
            
            try:
                codes = json.loads(response_text)
                
                # create one row per code
                for code_entry in codes:
                    results.append({
                        'url': url,
                        'date': date,
                        'publication': publication,
                        'code': code_entry.get('code'),
                        'code_5_sub': code_entry.get('code_5_sub'),
                        'justification': code_entry.get('justification', ''),
                        'key_quote': code_entry.get('key_quote', '')
                    })
                    
            except json.JSONDecodeError as je:
                print(f"json parse error for {url}, attempting to fix...")
                
                import re
                
                # Line-by-line approach to fix key_quote fields
                lines = response_text.split('\n')
                fixed_lines = []
                
                for line in lines:
                    if '"key_quote":' in line:
                        # Find the content after "key_quote": "
                        match = re.match(r'(\s*"key_quote":\s*")(.*)$', line)
                        if match:
                            prefix = match.group(1)  # keeps: "key_quote": "
                            rest = match.group(2)     # everything after
                            
                            # Remove all quotes from content except the very last one (closing quote)
                            if rest.endswith('"'):
                                content = rest[:-1].replace('"', '')  # remove all quotes except last
                                fixed_line = prefix + content + '"'
                                fixed_lines.append(fixed_line)
                            else:
                                fixed_lines.append(line)  # keep unchanged if no closing quote
                        else:
                            fixed_lines.append(line)  # keep unchanged if pattern doesn't match
                    else:
                        fixed_lines.append(line)  # keep all other lines unchanged
                
                fixed_response = '\n'.join(fixed_lines)
                
                try:
                    codes = json.loads(fixed_response)
                    
                    # create one row per code
                    for code_entry in codes:
                        results.append({
                            'url': url,
                            'date': date,
                            'publication': publication,
                            'code': code_entry.get('code'),
                            'code_5_sub': code_entry.get('code_5_sub'),
                            'justification': code_entry.get('justification', ''),
                            'key_quote': code_entry.get('key_quote', '')
                        })
                    print(f"  successfully fixed and parsed!")
                    
                except json.JSONDecodeError:
                    print(f"  could not fix, error: {str(je)}")
                    print(f"  response length: {len(full_response)} chars")
                    
                    # save full response to individual file for inspection
                    error_file = f"error_response_{custom_id}.txt"
                    with open(error_file, 'w', encoding='utf-8') as ef:
                        ef.write(full_response)
                    print(f"  full response saved to {error_file}")
                    
                    # also save the fixed attempt for debugging
                    with open(f"fixed_attempt_{custom_id}.txt", 'w', encoding='utf-8') as ef:
                        ef.write(fixed_response)
                    print(f"  fixed attempt saved to fixed_attempt_{custom_id}.txt")
                    
                    errors.append({
                        'url': url,
                        'custom_id': custom_id,
                        'error_type': 'json_parse_error',
                        'error_message': str(je),
                        'response_length': len(full_response),
                        'response': full_response
                    })
                    continue
        else:
            # detailed error information
            error_info = {
                'url': url,
                'custom_id': custom_id,
                'error_type': result.result.type,
                'error_details': str(result.result)
            }
            
            # try to get more specific error message
            if hasattr(result.result, 'error'):
                error_info['error_message'] = str(result.result.error)
            
            errors.append(error_info)
            print(f"error for {url}: {result.result.type}")
            print(f"  details: {error_info.get('error_message', 'no additional details')}")
    
    # write results to csv
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'date', 'publication', 'code', 'code_5_sub', 'justification', 'key_quote'])
        writer.writeheader()
        writer.writerows(results)
    
    # write errors to separate file if any
    if errors:
        error_file = output_file.replace('.csv', '_errors.json')
        with open(error_file, 'w', encoding='utf-8') as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)
        print(f"\n{len(errors)} errors occurred. details saved to {error_file}")
    
    print(f"\nbatch processing complete! {len(results)} codes assigned")
    print(f"results saved to {output_file}")
    
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='classify hagwon articles')
    parser.add_argument('input_file', help='input json with translated articles')
    parser.add_argument('--output', '-o', default='classified_articles.csv', help='output csv file')
    parser.add_argument('--batch', action='store_true', help='use batch api (50% discount)')
    parser.add_argument('--batch-file', default='batch_requests.jsonl', help='batch request file')
    parser.add_argument('--submit-batch', action='store_true', help='submit batch job')
    parser.add_argument('--process-batch', help='process completed batch by id')
    parser.add_argument('--estimate-only', action='store_true', help='only show cost estimate')
    
    args = parser.parse_args()
    
    # check for api key in environment
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("error: ANTHROPIC_API_KEY environment variable not set")
        return
    
    # always show cost estimate
    estimate_cost(args.input_file, use_batch=args.batch)
    
    if args.estimate_only:
        return
    
    # ask for confirmation
    response = input("proceed with classification? (yes/no): ")
    if response.lower() != 'yes':
        print("cancelled.")
        return
    
    if args.batch:
        create_batch_requests(args.input_file, args.batch_file)
        if args.submit_batch:
            submit_batch(args.batch_file)
        else:
            print(f"\nbatch file ready. submit with --submit-batch flag")
    elif args.process_batch:
        process_batch_results(args.process_batch, args.input_file, args.output)
    else:
        classify_articles_standard(args.input_file, args.output)

if __name__ == "__main__":
    main()