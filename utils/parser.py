import re

def parse_sql(text):
    pattern = r'(?:SELECT|WITH)(?:[^;`]|`(?!``))*(?:;|```)'
    query_candidates = re.findall(pattern, text, re.DOTALL| re.MULTILINE)
    if len(query_candidates) == 0:
        return 'no queries'
    else:
        the_query = query_candidates[0]
        return the_query.replace('```','')
    
def parse_sql_intro(text):
    pattern = r'(?:SELECT|WITH)'
    match = re.search(pattern, text)
    
    if not match:
        return text  # Return full text if no query found
    
    intro_text = text[:match.start()].strip()
    
    intro_text = intro_text.replace('```sql', '').replace('```', '').strip()
    
    return intro_text if intro_text else 'no introduction'

def parse_code_blocks(text):
    pattern = r'```(?:\w+\n)?(.*?)```'
    code_blocks = re.findall(pattern, text, re.DOTALL)
    return [block.strip() for block in code_blocks]