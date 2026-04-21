import os, requests, time, json
from dotenv import load_dotenv
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN", "")
GEMINI_KEY  = os.getenv("GEMINI_API_KEY", "")
GEMINI_BASE = "https://generativelanguage.googleapis.com"
APIFY_BASE  = "https://api.apify.com/v2"

DATASET_ID  = "cVehBiJcaThxM3vuj"
REEL_URL    = "https://www.instagram.com/reel/DXWt7p2jDJG/"

items = requests.get(
    f"{APIFY_BASE}/datasets/{DATASET_ID}/items",
    params={"token": APIFY_TOKEN, "limit": 300},
    timeout=30,
).json()

comments = [c.get("text") or c.get("comment") or "" for c in items if (c.get("text") or c.get("comment"))]
total    = 216  # known from post

print(f"Reel  : {REEL_URL}")
print(f"Total comments on reel: {total} | Analysing: {len(comments)}")

joined = "\n".join(f"- {c}" for c in comments[:300])

prompt = (
    "You are @foodpharmer — India's most trusted food myth-busting creator with 3.4M Instagram followers. "
    "You expose harmful ingredients, misleading labels, and food industry scams. "
    "You are reading the comment section of your own Instagram Reel.\n\n"
    f"Reel URL: {REEL_URL}\n"
    "Reel context: This reel is about child birthday parties becoming social media events — "
    "designer cakes, professional photographers, premium return gifts, huge crowds, sugar/junk food given to "
    "1-year-olds who don't understand any of it. The message questions whether this is about the child or "
    "the parents' social media validation.\n"
    f"Total comments on reel: {total}\n"
    f"Comments analysed: {len(comments)}\n\n"
    "Classify each comment from YOUR perspective — 'Is this comment good or bad FOR MY MESSAGE?'\n\n"
    "POSITIVE: validates your message — agreement, gratitude, sharing, personal stories of social pressure "
    "at events, concern for child health (sugar/overstimulation), calling out parent vanity/social media culture, "
    "tagging friends/family, requests for more such content — all mean your message LANDED\n"
    "NEGATIVE: attacks YOU or YOUR content — 'judgemental', 'your business', 'misinformation', "
    "'let parents celebrate', directly discrediting you or telling you to mind your own business\n"
    "NEUTRAL: questions, mixed feelings, personal celebrations they still plan, requests for clarification\n\n"
    "After classifying, give me:\n"
    "1. Sentiment score (positive_count / total_count)\n"
    "2. Top 5 positive themes\n"
    "3. Top 3 negative themes\n"
    "4. Top 3 neutral themes\n"
    "5. One paragraph: what is this audience most emotionally activated by? "
    "What angle should the next video double down on?\n"
    "6. Top 3 actionables for @foodpharmer based purely on comment signals\n"
    "7. Any surprising patterns in the comments\n\n"
    "Reply ONLY with valid JSON:\n"
    "{\n"
    '  "score": <float 0-1>,\n'
    '  "positive_count": <int>,\n'
    '  "negative_count": <int>,\n'
    '  "neutral_count": <int>,\n'
    '  "positive_themes": ["theme1", "..."],\n'
    '  "negative_themes": ["theme1", "..."],\n'
    '  "neutral_themes": ["theme1", "..."],\n'
    '  "audience_activation": "One paragraph on what this audience is most emotionally activated by",\n'
    '  "top_actionables": ["ACTIONABLE 1: ...", "ACTIONABLE 2: ...", "ACTIONABLE 3: ..."],\n'
    '  "surprising_patterns": "Any surprising or unexpected patterns in the comments",\n'
    '  "summary": "One punchy sentence on overall audience reaction"\n'
    "}\n\n"
    f"COMMENTS:\n{joined}"
)

for model in ["gemini-2.5-pro", "gemini-2.5-flash"]:
    for attempt in range(2):
        resp = requests.post(
            f"{GEMINI_BASE}/v1beta/models/{model}:generateContent",
            params={"key": GEMINI_KEY},
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0.1, "maxOutputTokens": 8192, "thinkingConfig": {"thinkingBudget": 0}},
            },
            timeout=120,
        )
        if resp.status_code in (500, 503):
            wait = 15 * (2 ** attempt)
            print(f"  {resp.status_code} ({model}) — retrying in {wait}s...")
            time.sleep(wait)
            continue
        break
    if resp.ok:
        print(f"  Model used: {model}")
        break
    print(f"  {model} failed ({resp.status_code}) — trying next model...")

resp.raise_for_status()
resp_json = resp.json()
candidate = resp_json.get("candidates", [{}])[0]
if "content" not in candidate or "parts" not in candidate.get("content", {}):
    print("Unexpected response:", json.dumps(resp_json, indent=2)[:800])
    raise SystemExit(1)
parts = candidate["content"]["parts"]
raw   = next((p["text"] for p in reversed(parts) if not p.get("thought")), parts[-1]["text"]).strip()
raw   = raw.replace("```json", "").replace("```", "").strip()
start, end = raw.find("{"), raw.rfind("}")
if start != -1 and end > start:
    raw = raw[start:end+1]

try:
    result = json.loads(raw)
except json.JSONDecodeError:
    open_braces   = raw.count("{") - raw.count("}")
    open_brackets = raw.count("[") - raw.count("]")
    raw += "]" * max(0, open_brackets) + "}" * max(0, open_braces)
    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"JSON parse failed: {e}")
        print("Raw (first 500 chars):", raw[:500])
        raise SystemExit(1)

print()
print("=" * 60)
print(f"  REEL  : {REEL_URL}")
print("=" * 60)
print(f"  Sentiment  : {result['score']:.0%}")
print(f"  Positive   : {result['positive_count']}")
print(f"  Negative   : {result['negative_count']}")
print(f"  Neutral    : {result['neutral_count']}")
print()
print("  POSITIVE THEMES:")
for t in result.get("positive_themes", []):   print(f"    + {t}")
print()
print("  NEGATIVE THEMES:")
for t in result.get("negative_themes", []):   print(f"    - {t}")
print()
print("  NEUTRAL THEMES:")
for t in result.get("neutral_themes", []):    print(f"    ~ {t}")
print()
print("  AUDIENCE ACTIVATION:")
print(f"  {result.get('audience_activation', '')}")
print()
print("  TOP ACTIONABLES:")
for a in result.get("top_actionables", []):   print(f"  {a}")
print()
print("  SURPRISING PATTERNS:")
print(f"  {result.get('surprising_patterns', '')}")
print()
print(f"  SUMMARY: {result.get('summary', '')}")
print("=" * 60)
