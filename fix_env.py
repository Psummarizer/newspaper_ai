
import re
import json

env_path = '.env'

try:
    with open(env_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Regex to find FIREBASE_CREDENTIALS_JSON=... until the end of a block
    # We assume it starts with { and ends with }
    # This is a bit tricky if there are multiple vars. 
    # Let's assume the user pasted it at the end or it's distinct.
    
    # Matches FIREBASE_CREDENTIALS_JSON= and capturing everything that looks like a JSON object spanning lines
    # Dotall is needed to span lines.
    pattern = re.compile(r'(FIREBASE_CREDENTIALS_JSON\s*=\s*)(\{.*?\})', re.DOTALL | re.MULTILINE)
    
    match = pattern.search(content)
    if match:
        prefix = match.group(1)
        json_str = match.group(2)
        
        try:
            # Parse to ensure it's valid and get dict
            data = json.loads(json_str)
            # Dump back to string without indents/newlines
            compact_json = json.dumps(data)
            
            # Replace in original content
            new_content = content.replace(json_str, "'" + compact_json + "'") # Wrap in quotes just in case, though usually simple assignment works if no spaces/special chars break it. 
            # Actually, for dotenv, values with spaces should be quoted. JSON definitely has spaces.
            
            # Rewrite file
            with open(env_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
                
            print("Successfully flattened FIREBASE_CREDENTIALS_JSON.")
            
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON found: {e}")
            # Fallback: try to just remove newlines
            compact_txt = json_str.replace('\n', '').replace('\r', '')
            new_content = content.replace(json_str, "'" + compact_txt + "'")
            with open(env_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print("Fallback: Newlines removed (raw string reqrite).")
            
    else:
        print("FIREBASE_CREDENTIALS_JSON not found or regex failed.")

except Exception as e:
    print(f"Failed to process .env: {e}")
