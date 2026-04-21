"""
@foodpharmer — Instagram Daily Performance Report

Run manually : python scraper_dev.py
"""

import json, os, smtplib, time, calendar, tempfile
import requests
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

# ── Credentials ───────────────────────────────────────────────────────────────
APIFY_TOKEN = os.getenv("APIFY_TOKEN",    "")
GEMINI_KEY  = os.getenv("GEMINI_API_KEY", "")
GROQ_KEY    = os.getenv("GROQ_API_KEY",   "")
GMAIL_USER  = os.getenv("GMAIL_USER",     "")
GMAIL_PASS  = os.getenv("GMAIL_APP_PASS", "")

# ── Config ────────────────────────────────────────────────────────────────────
IG_USERNAME    = "foodpharmer"
COMMENTS_LIMIT = 1000
HISTORY_LIMIT  = 10
IG_HISTORY     = "history_ig.json"
FOLLOWER_LOG   = "followers_ig.json"
SNAPSHOTS_FILE = "snapshots_ig.json"   # per-post view/like snapshots over time
APIFY_BASE     = "https://api.apify.com/v2"

# ── Growth targets ────────────────────────────────────────────────────────────
WEEKLY_TARGET  = 50_000
MONTHLY_TARGET = 200_000

# ── Historical CSV paths (optional bootstrap — place next to script) ──────────
DAILY_CSV_PATH   = "daily_cumulative.csv"
MONTHLY_CSV_PATH = "monthly_growth.csv"

# ── Dev mode — set True to send only to dev email ─────────────────────────────
DEV_MODE = False
DEV_EMAIL = "dev.narsinghani@gmail.com"

# ── Recipients ────────────────────────────────────────────────────────────────
_EMAIL_TO_ALL = [
    "dhairyavora4@gmail.com",
    "manufilmwala@gmail.com",
    "foodpharmer@gmail.com",
    "samvida.patel@nyu.edu",
    "shivamagwl02@gmail.com",
    "harshdas199@gmail.com",
    "chiragofficial2006@gmail.com",
    "dhritigupta70@gmail.com",
    "dev.narsinghani@gmail.com",
]
EMAIL_TO = [DEV_EMAIL] if DEV_MODE else _EMAIL_TO_ALL

# ── Reddit config ─────────────────────────────────────────────────────────────
REDDIT_KEYWORDS      = ["food pharmer", "Revant Himatsingka", "foodpharmer", "only what's needed whey protein", "OWN whey protein", "OWN whey", "only what's needed whey"]
# Only posts/comments that contain at least one of these strings (case-insensitive)
# will pass the relevance filter — prevents generic keyword matches from leaking in
REDDIT_RELEVANCE_TERMS = ["food pharmer", "foodpharmer", "revant", "himatsingka"]
REDDIT_BASE          = "https://www.reddit.com/search.json"
REDDIT_HEADERS       = {"User-Agent": "FoodPharmerDigestBot/1.0"}
REDDIT_LOOKBACK_HOURS = 48   # look back 48 h; filters posts older than this

# ═══════════════════════════════════════════════════════════════════════════════
# APIFY
# ═══════════════════════════════════════════════════════════════════════════════

def _apify_get_with_retry(url: str, params: dict, timeout: int = 20,
                          max_retries: int = 5, backoff: float = 10.0):
    """GET with exponential backoff — survives transient DNS / network blips."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            if attempt == max_retries - 1:
                raise
            wait = backoff * (2 ** attempt)
            print(f"      ⚠ Network error ({e.__class__.__name__}), retrying in {int(wait)}s...")
            time.sleep(wait)


def run_apify_actor(actor_id: str, run_input: dict, label: str) -> list:
    print(f"    -> [{actor_id}] {label}")
    r = requests.post(
        f"{APIFY_BASE}/acts/{actor_id}/runs",
        params={"token": APIFY_TOKEN},
        json=run_input, timeout=40,
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    status = "RUNNING"
    for attempt in range(120):
        time.sleep(5)
        s = _apify_get_with_retry(
            f"{APIFY_BASE}/actor-runs/{run_id}",
            params={"token": APIFY_TOKEN},
        )
        status = s.json()["data"]["status"]
        if attempt % 6 == 0:
            print(f"      ... {status}")
        if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
            break
    if status != "SUCCEEDED":
        raise RuntimeError(f"Apify run ended: {status}")
    dataset_id = s.json()["data"]["defaultDatasetId"]
    items = _apify_get_with_retry(
        f"{APIFY_BASE}/datasets/{dataset_id}/items",
        params={"token": APIFY_TOKEN, "limit": 500},
        timeout=40,
    ).json()
    print(f"      ✓ {len(items)} items")
    return items


# ═══════════════════════════════════════════════════════════════════════════════
# INSTAGRAM SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_instagram():
    print("\n[1/3] Fetching Instagram profile + posts...")

    # Profile details (followers)
    followers = None
    try:
        prof = run_apify_actor(
            "apify~instagram-scraper",
            {"directUrls": [f"https://www.instagram.com/{IG_USERNAME}/"],
             "resultsType": "details", "resultsLimit": 1},
            "profile",
        )
        if prof:
            p = prof[0]
            followers = (p.get("followersCount") or p.get("followers") or
                         p.get("edge_followed_by", {}).get("count"))
            print(f"      Followers: {followers:,}" if followers else "      Followers: n/a")
    except Exception as e:
        print(f"      Warning: profile fetch failed — {e}")

    # Posts
    items = run_apify_actor(
        "apify~instagram-scraper",
        {"directUrls": [f"https://www.instagram.com/{IG_USERNAME}/"],
         "resultsType": "posts", "resultsLimit": 20},
        "posts",
    )

    posts = []
    for p in items:
        ts = p.get("timestamp") or p.get("taken_at_timestamp")
        dt = _parse_ts(ts)
        sc = p.get("shortCode") or p.get("shortcode") or p.get("id", "")
        likes    = p.get("likesCount")    or p.get("likes")    or 0
        comments = p.get("commentsCount") or p.get("comments") or 0
        is_video = p.get("type") in ("Video", "Reel") or bool(p.get("videoUrl")) or bool(p.get("videoDuration"))
        views    = (p.get("videoPlayCount") or p.get("videoViewCount") or
                    p.get("playCount")      or p.get("video_view_count") or None)
        shares    = p.get("sharesCount")   or p.get("shares")   or p.get("shareCount") or None
        video_url = p.get("videoUrl")      or p.get("video_url") or None

        posts.append({
            "id":         sc,
            "url":        p.get("url") or f"https://www.instagram.com/p/{sc}/",
            "caption":    (p.get("caption") or "")[:200],
            "date":       dt.strftime("%d %b %Y"),
            "date_ts":    dt.timestamp(),
            "likes":      likes,
            "comments":   comments,
            "views":      views,
            "shares":     shares,
            "engagement": round((likes + comments) / followers * 100, 2) if followers else None,
            "thumb":      p.get("displayUrl") or p.get("thumbnailUrl") or "",
            "is_video":   is_video,
            "video_url":  video_url,
        })
        print(f"        {sc} | {dt.strftime('%d %b')} | likes={likes} | views={views} | type={p.get('type')} | video_url={'YES' if video_url else 'MISSING'}")
        if not video_url and is_video:
            video_keys = {k: v for k, v in p.items() if "video" in k.lower() or "url" in k.lower()}
            print(f"          [DEBUG] video-related keys returned by Apify: {video_keys}")

    # Strict date filter — exclude pinned posts older than 90 days
    now_ts = datetime.now(timezone.utc).timestamp()
    posts  = [p for p in posts if (now_ts - p["date_ts"]) <= 90 * 86400]
    posts.sort(key=lambda x: x["date_ts"], reverse=True)
    posts = posts[:12]
    if not posts:
        raise RuntimeError("No recent posts found (all posts older than 90 days)")
    latest = posts[0]
    latest["followers"] = followers
    print(f"      Latest post: {latest['id']} | likes={latest['likes']} | {latest['date']}")

    # Comments for latest post
    try:
        citems = run_apify_actor(
            "apify~instagram-comment-scraper",
            {"directUrls": [latest["url"]], "resultsLimit": COMMENTS_LIMIT, "maxComments": COMMENTS_LIMIT},
            "comments",
        )
        latest["comment_texts"] = [
            c.get("text") or c.get("content") or ""
            for c in citems if c.get("text") or c.get("content")
        ][:COMMENTS_LIMIT]
    except Exception as e:
        print(f"      Warning: comments failed — {e}")
        latest["comment_texts"] = []

    return latest, posts


# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI SENTIMENT
# ═══════════════════════════════════════════════════════════════════════════════

def analyse(comments: list, caption: str) -> dict:
    if not comments:
        return {"positive": [], "negative": [], "neutral": [],
                "score": 0.5, "summary": "No comments fetched."}
    joined = "\n".join(f"- {c}" for c in comments[:1000])
    total  = len(comments[:1000])

    prompt = f"""You are @foodpharmer — an Indian food/health myth-busting creator with 3M+ followers. You expose harmful ingredients, misleading labels, and failures of the food industry and regulators. You are reading your own comment section after posting a new video.

Post caption: "{caption[:150]}"

Your job: classify each of the {total} comments below from YOUR perspective as the creator. Ask yourself — "Is this comment good or bad FOR MY MESSAGE?"

STEP 1 — Classify each comment into one bucket:
- POSITIVE: anything that validates your message or shows the viewer is on your side — gratitude, praise, agreement, sharing with family, success stories, tagging friends, outrage/shock/disappointment at food companies or brands, anger at FSSAI/government/regulators for not acting, fire/heart emojis — ALL of these mean your message landed
- NEGATIVE: ONLY comments that attack YOU or YOUR content directly — "this is wrong", "you are spreading misinformation", "this is fake", "he is lying", "bad advice", "don't trust him" — i.e. someone is trying to discredit you or your message
- NEUTRAL: questions, genuine curiosity, requests for more info, personal stories, polite disagreement with a specific fact (not an attack)

STEP 2 — Count how many comments fall into each bucket.

STEP 3 — Calculate score as: positive_count / total_count. Round to 2 decimal places. Do NOT default to 0.85.

STEP 4 — Extract up to 5 short theme phrases from positive comments, up to 3 from negative, up to 3 from neutral.

Reply ONLY with valid JSON, no markdown, no explanation:
{{
  "positive": ["theme1", "theme2"],
  "negative": ["theme1"],
  "neutral":  ["theme1"],
  "score":    <calculated float, NOT 0.85>,
  "summary":  "One sentence on overall audience reaction"
}}"""

    raw = ""
    try:
        raw = _gemini_text(prompt, max_tokens=800, temperature=0.1)
        return json.loads(raw)
    except Exception as e:
        print(f"      Sentiment analysis error: {type(e).__name__}: {e}")
        return {"positive": [], "negative": [], "neutral": [],
                "score": 0.75, "summary": "Analysis failed."}


# ═══════════════════════════════════════════════════════════════════════════════
# GEMINI VIDEO ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════

GEMINI_BASE  = "https://generativelanguage.googleapis.com"
GEMINI_MODEL = "gemini-2.5-pro"   # video analysis only
GROQ_BASE    = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL   = "llama-3.3-70b-versatile"


def _gemini_text(prompt: str, max_tokens: int = 1000, temperature: float = 0.2, timeout: int = 60) -> str:
    """Call Groq for text-only generation (sentiment, Reddit, explainer)."""
    for attempt in range(3):
        resp = requests.post(
            GROQ_BASE,
            headers={"Authorization": f"Bearer {GROQ_KEY}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            timeout=timeout,
        )
        if resp.status_code in (429, 500, 503):
            wait = 10 * (2 ** attempt)
            print(f"      Groq {resp.status_code} — retrying in {wait}s...")
            time.sleep(wait)
            continue
        break
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"].strip()
    start, end = raw.find("{"), raw.rfind("}")
    if start != -1 and end > start:
        return raw[start:end + 1]
    return raw.replace("```json", "").replace("```", "").strip()


def _upload_video_to_gemini(video_path: str) -> str:
    """Upload video to Gemini Files API using resumable upload. Returns file URI."""
    size_bytes = os.path.getsize(video_path)
    size_mb    = size_bytes / (1024 * 1024)
    print(f"      📤 Uploading {size_mb:.1f} MB to Gemini Files API...")

    # Start resumable upload session
    start = requests.post(
        f"{GEMINI_BASE}/upload/v1beta/files",
        params={"key": GEMINI_KEY, "uploadType": "resumable"},
        headers={
            "X-Goog-Upload-Protocol":             "resumable",
            "X-Goog-Upload-Command":              "start",
            "X-Goog-Upload-Header-Content-Length": str(size_bytes),
            "X-Goog-Upload-Header-Content-Type":   "video/mp4",
            "Content-Type":                        "application/json",
        },
        json={"file": {"display_name": os.path.basename(video_path)}},
        timeout=30,
    )
    start.raise_for_status()
    upload_url = start.headers["X-Goog-Upload-URL"]

    # Upload bytes
    with open(video_path, "rb") as f:
        video_bytes = f.read()

    finish = requests.post(
        upload_url,
        headers={
            "Content-Length":        str(size_bytes),
            "X-Goog-Upload-Offset":  "0",
            "X-Goog-Upload-Command": "upload, finalize",
        },
        data=video_bytes,
        timeout=300,
    )
    finish.raise_for_status()
    file_info = finish.json()
    file_uri  = file_info["file"]["uri"]
    file_name = file_info["file"]["name"]
    print(f"      ✓ Uploaded — waiting for Gemini to process...")

    # Poll until ACTIVE
    for _ in range(40):
        time.sleep(5)
        status = requests.get(
            f"{GEMINI_BASE}/v1beta/{file_name}",
            params={"key": GEMINI_KEY},
            timeout=15,
        ).json()
        state = status.get("state")
        if state == "ACTIVE":
            print("      ✓ Video ready")
            return file_uri
        if state == "FAILED":
            raise RuntimeError("Gemini file processing failed")
    raise RuntimeError("Gemini file processing timed out")


def analyse_video(post: dict) -> dict | None:
    """
    Downloads the Reel, uploads it to Gemini Files API, and uses Gemini's
    native video understanding for deep content analysis.
    Returns a structured dict or None if not applicable / on error.
    """
    if not post.get("is_video"):
        return None
    video_url = post.get("video_url")
    if not video_url:
        print("      ℹ️  Video post but no video_url — skipping video analysis")
        return None
    if not GEMINI_KEY:
        print("      ⚠️  GEMINI_API_KEY not set — skipping video analysis")
        return None

    print("      📥 Downloading video for analysis...")
    tmp_path = None

    try:
        # ── 1. Download video ─────────────────────────────────────────────────
        resp = requests.get(video_url, timeout=120, stream=True,
                            headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                tmp.write(chunk)
            tmp_path = tmp.name

        size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
        print(f"      ✓ Downloaded {size_mb:.1f} MB")

        # ── 2. Upload to Gemini Files API ─────────────────────────────────────
        file_uri = _upload_video_to_gemini(tmp_path)

        # ── 3. Build prompt ───────────────────────────────────────────────────
        caption_snippet = post.get("caption", "")[:200]
        likes = post.get("likes", 0)
        views = post.get("views") or 0

        prompt = f"""You are Instagram's top growth specialist — you have spent years studying what makes short-form video go viral on Instagram Reels, specifically in the Indian health/consumer awareness space. You have reverse-engineered hundreds of @foodpharmer's videos and know exactly why each one succeeded or failed.

@foodpharmer is India's most-watched food myth-busting creator — 3.4M followers, videos exposing harmful ingredients, misleading labels, and food industry scams. His BEST reels hit 10M–26M views. You are being paid to watch this video and give brutally honest, deeply specific feedback that the team can act on TOMORROW.

WHAT MAKES FOODPHARMER'S TOP 1% REELS WORK:
- HOOK (first 2 seconds): A shocking stat, forbidden ingredient, or provocative claim in large high-contrast on-screen text. The viewer must feel "wait, what?" before they can scroll. e.g. "This chemical is in 90% of Indian kitchens and slowly destroys your liver"
- STRUCTURE: Problem (shock) → Evidence (proof/label/study) → Solution or warning. Zero filler. Every cut earns its place.
- EMOTION: The viewer must feel either BETRAYED ("I've been lied to") or URGENCY ("I need to share this right now"). Informative alone doesn't go viral. Emotional + informative does.
- INDIAN SPECIFICITY: Names actual Indian brands (Maggi, Bournvita, Amul, Fortune, MDH, Haldiram's), foods (chai, roti, namkeen, biscuits), or habits every Indian household recognises. Generic health content doesn't perform — brand-specific content does.
- TEXT OVERLAYS: Critical facts must appear as bold on-screen text, not just spoken. 40%+ of viewers watch on mute.
- SHOCK SEQUENCES: Deliberately long sequences showing sugar quantities, ingredient lists, or comparison scrolls are an INTENTIONAL and PROVEN format for this account — do NOT penalise these as "too slow" or "overwhelming". They ARE the shock moment. e.g. showing 10 products one by one with their sugar content builds cumulative outrage that drives shares.
- SHAREABILITY: Must have a clear "tag your mum" or "save this before they delete it" moment. If the viewer wouldn't forward it in a family WhatsApp group, it won't go viral.
- CTA: "Save this" or "Tag someone who eats X" massively outperforms "follow me for more".

WHAT KILLS PERFORMANCE:
- Hook that takes more than 3 seconds to land
- Starting with context/background instead of the shocking claim
- No on-screen text for key facts
- Topic too niche, too scientific, or not emotionally resonant with middle-class Indian families
- No shareable moment — viewer learns something but doesn't feel compelled to pass it on
- Weak or generic CTA

IMPORTANT — DO NOT PENALISE THESE (they are intentional):
- Long sequences showing sugar/ingredient quantities across multiple products — this IS the shock content, it builds cumulative outrage
- Repeated on-screen numbers or comparisons — deliberate format to make the viewer feel the scale of the problem
- Slow reveal of data points one by one — this is pacing by design, not a flaw

ACCOUNT CONTEXT:
- This reel got {views:,} views and {likes:,} likes
- Performance verdict: {'significantly underperforming — this is in the bottom tier of the account' if views < 1_000_000 else 'performing well above average' if views > 5_000_000 else 'average performance'}
- Account median: ~2M views per reel
- Caption: "{caption_snippet}"

Watch every second of this video. Then give me your full specialist breakdown. For every weakness you identify, give me the EXACT fix — not "improve the hook" but "replace the opening line with: [exact text]". Not "add a CTA" but "at the 45-second mark, add: [exact words to say/show]".

Respond ONLY with valid JSON — no markdown, no explanation:
{{
  "hook": {{
    "score": <1-10>,
    "timestamp_seconds": <exact second when the hook lands>,
    "what_it_is": "Describe word-for-word or frame-by-frame what the actual opening hook is",
    "verdict": "Does it make someone stop scrolling in under 2 seconds? Why or why not?",
    "exact_fix": "If score < 8: write the exact replacement hook text/visual they should use instead"
  }},
  "retention": {{
    "score": <1-10>,
    "drop_off_risk": "At what timestamp does viewer interest likely peak and start dropping, and why?",
    "fix": "What specific change at that timestamp would keep viewers watching?"
  }},
  "pacing": {{
    "rating": "<too fast | good | too slow>",
    "note": "Specific timestamps where pacing hurts or helps"
  }},
  "visuals": {{
    "score": <1-10>,
    "note": "Video quality, lighting, framing — what's working and what's not"
  }},
  "text_overlays": {{
    "rating": "<effective | missing | overwhelming | none>",
    "missing_facts": "Which specific facts in this video should have appeared as on-screen text but didn't?",
    "fix": "List the exact text overlays to add and at which timestamps"
  }},
  "audio": {{
    "rating": "<strong | adequate | weak | none>",
    "note": "Voiceover clarity, energy level, background music — specific feedback"
  }},
  "emotion_trigger": {{
    "score": <1-10>,
    "type": "<betrayal | urgency | shock | curiosity | none>",
    "note": "What emotion does this video create, and is it strong enough to drive shares?"
  }},
  "shareability": {{
    "score": <1-10>,
    "whatsapp_moment": "Is there a specific moment a viewer would forward to their family WhatsApp group? What is it or what's missing?",
    "fix": "Exact line or visual to add that creates a share trigger"
  }},
  "indian_context": {{
    "score": <1-10>,
    "brands_mentioned": ["list any Indian brands or foods mentioned"],
    "missed_opportunities": "Which Indian brands, foods, or habits could have been referenced to make this more relatable?"
  }},
  "cta": {{
    "present": <true | false>,
    "what_was_said": "Exact CTA used in the video",
    "verdict": "Is this the right CTA for this content?",
    "better_cta": "The exact CTA line that would perform better for this specific video"
  }},
  "overall_score": <1-10>,
  "why_it_underperformed": "3-4 sentences identifying the PRIMARY reasons this reel didn't hit 2M+ views — be brutally specific",
  "top_3_actionables": [
    "ACTIONABLE 1 — [Category]: [Exact change to make] — e.g. HOOK: Replace opening line with 'X chemical found in Y% of Indian homes causes Z' shown as white text on black for first 2 seconds",
    "ACTIONABLE 2 — [Category]: [Exact change to make]",
    "ACTIONABLE 3 — [Category]: [Exact change to make]"
  ],
  "if_i_were_editing_this": "2-3 sentences on what you would personally do differently if you were re-editing this reel from scratch",
  "summary": "One punchy sentence — the single biggest reason this video did or didn't perform"
}}"""

        # ── 4. Call Gemini (with retry for transient 5xx) ─────────────────────
        print(f"      🤖 Sending to Gemini ({GEMINI_MODEL}) for analysis...")
        resp_g = None
        for attempt in range(4):
            resp_g = requests.post(
                f"{GEMINI_BASE}/v1beta/models/{GEMINI_MODEL}:generateContent",
                params={"key": GEMINI_KEY},
                json={
                    "contents": [{
                        "parts": [
                            {"text": prompt},
                            {"file_data": {"mime_type": "video/mp4", "file_uri": file_uri}},
                        ]
                    }],
                    "generationConfig": {"temperature": 0.2, "maxOutputTokens": 8192},
                },
                timeout=120,
            )
            if resp_g.status_code in (500, 503):
                wait = 15 * (2 ** attempt)
                print(f"      Gemini {resp_g.status_code} — retrying in {wait}s (attempt {attempt + 1}/4)...")
                time.sleep(wait)
                continue
            break
        if not resp_g.ok:
            print(f"      Gemini error {resp_g.status_code}: {resp_g.text[:300]}")
        resp_g.raise_for_status()

        # Gemini 2.5 returns thinking tokens as separate parts — get the last non-thought part
        parts = resp_g.json()["candidates"][0]["content"]["parts"]
        raw = next((p["text"] for p in reversed(parts) if not p.get("thought")), parts[-1]["text"]).strip()
        # Extract JSON object robustly — strips markdown fences and any preamble
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end != -1:
            raw = raw[start:end + 1]
        else:
            raw = raw.replace("```json", "").replace("```", "").strip()

        result = json.loads(raw)
        print(f"      ✓ Video analysis complete — overall score: {result.get('overall_score')}/10")
        return result

    except json.JSONDecodeError:
        print("      Warning: Gemini returned non-JSON — storing raw text")
        return {"summary": raw[:300], "recommendations": [], "overall_score": None}
    except Exception as e:
        print(f"      Warning: video analysis failed — {e}")
        return None
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


# ═══════════════════════════════════════════════════════════════════════════════
# POST PERFORMANCE EXPLAINER
# ═══════════════════════════════════════════════════════════════════════════════

def _detect_content_signals(caption: str) -> list:
    caption_lower = caption.lower()
    signals = []
    if "shocking reality" in caption_lower or "shocking" in caption_lower:
        signals.append("SHOCKING format")
    if "#ad" in caption_lower or "sponsored" in caption_lower:
        signals.append("branded/ad content")
    if any(x in caption_lower for x in ["parliament", "politician", "government", "court case", "mp ", "mla "]):
        signals.append("political angle")
    if any(x in caption_lower for x in ["dr.", "doctor", "sivaranjani", "arun"]):
        signals.append("doctor/expert story")
    if any(x in caption_lower for x in ["our product", "plant protein", "whey protein", "order link", "in stock", "out of stock"]):
        signals.append("own brand / product post")
    if any(x in caption_lower for x in ["share", "spread awareness", "request to all"]):
        signals.append("shareability hook")
    if any(x in caption_lower for x in ["india", "indian", "₹", "crore", "lakh"]):
        signals.append("India-specific framing")
    if "%" in caption:
        signals.append("stat/percentage hook")
    if any(x in caption_lower for x in ["kids", "children", "school", "generation"]):
        signals.append("parenting/kids angle")
    return signals


def _classify_performance(post: dict, stats: dict) -> tuple:
    views     = post.get("views")
    eng       = post.get("engagement")
    likes     = post.get("likes", 0)
    med_views = stats.get("med_views")
    med_eng   = stats.get("med_engagement")
    med_likes = stats.get("med_likes")

    scores = []
    if views and med_views and med_views > 0:
        scores.append(views / med_views)
    if eng and med_eng and med_eng > 0:
        scores.append(eng / med_eng)
    if likes and med_likes and med_likes > 0:
        scores.append(likes / med_likes)

    if not scores:
        return ("Insufficient data", "❓", "#f3f4f6", "#374151")

    ratio = sum(scores) / len(scores)
    if ratio >= 2.0: return ("Viral 🔥",       "🔥", "#fef3c7", "#92400e")
    if ratio >= 1.3: return ("Above average",   "✅", "#dcfce7", "#166534")
    if ratio >= 0.7: return ("On par",         "〰️", "#f3f4f6", "#374151")
    return              ("Underperformed",      "📉", "#fee2e2", "#991b1b")


def explain_performance(post: dict, stats: dict) -> dict:
    label, emoji, bg, fg = _classify_performance(post, stats)
    signals = _detect_content_signals(post.get("caption", ""))

    med_views = stats.get("med_views")
    med_eng   = stats.get("med_engagement")
    med_likes = stats.get("med_likes")

    views_line = (f"  Views: {post.get('views', 'N/A'):,} (median: {med_views:,})"
                  if post.get("views") and med_views else "  Views: N/A")
    eng_line   = (f"  Engagement: {post.get('engagement', 'N/A')}% (median: {med_eng}%)"
                  if post.get("engagement") and med_eng else "")
    likes_line = (f"  Likes: {post['likes']:,} (median: {med_likes:,})"
                  if post.get("likes") and med_likes else "")

    signals_str = ", ".join(signals) if signals else "no strong signals detected"
    hours = post.get("hours_since_post", "unknown")

    prompt = f"""You are an Instagram growth analyst for @foodpharmer, an Indian food health awareness account with 3.2M+ followers.

Post performance verdict: {label}
Post age: {hours}h
Caption (first 200 chars): "{post.get('caption', '')[:200]}"

Metrics vs account median:
{likes_line}
{views_line}
{eng_line}

Content signals detected: {signals_str}

In EXACTLY 2-3 sentences, explain WHY this post {('performed well' if label in ('Viral 🔥', 'Above average') else 'underperformed or was average')}.
Be specific — reference the content format, caption hook, topic, or audience psychology. Do NOT just restate the numbers.
Keep it punchy and actionable.

Reply ONLY with the explanation text. No JSON, no bullet points, no markdown."""

    try:
        explanation = _gemini_text(prompt, max_tokens=300, temperature=0.3)
    except Exception as e:
        print(f"      Warning: performance explainer failed — {e}")
        explanation = f"Post classified as '{label}' based on metrics vs account median."

    return {"label": label, "emoji": emoji, "bg": bg, "fg": fg,
            "explanation": explanation, "signals": signals}


# ═══════════════════════════════════════════════════════════════════════════════
# HISTORY & STATS
# ═══════════════════════════════════════════════════════════════════════════════

def load_json(path: str, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def bootstrap_follower_log_from_csv(follower_log: list) -> list:
    if len(follower_log) >= 7:
        return follower_log
    if not os.path.exists(DAILY_CSV_PATH):
        return follower_log
    try:
        import csv
        existing_dates = {f["date"] for f in follower_log}
        new_entries = []
        with open(DAILY_CSV_PATH, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                date_str = row["date"].strip()
                if date_str not in existing_dates:
                    try:
                        new_entries.append({"date": date_str,
                                            "followers": int(row["cumulative_followers"])})
                    except (ValueError, KeyError):
                        continue
        if new_entries:
            follower_log = follower_log + new_entries
            follower_log.sort(key=lambda x: x["date"])
            cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
            follower_log = [f for f in follower_log if f["date"] >= cutoff]
            print(f"      ✓ Bootstrapped follower log with {len(new_entries)} entries from CSV")
    except Exception as e:
        print(f"      Warning: CSV bootstrap failed — {e}")
    return follower_log


def load_monthly_csv_context() -> dict:
    if not os.path.exists(MONTHLY_CSV_PATH):
        return {}
    try:
        import csv
        rows = []
        with open(MONTHLY_CSV_PATH, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rows.append({"year_month":    row["year_month"].strip(),
                                 "new_followers": int(row["new_followers"])})
                except (ValueError, KeyError):
                    continue
        if not rows:
            return {}
        rows.sort(key=lambda x: x["year_month"])
        best       = max(rows, key=lambda x: x["new_followers"])
        recent6    = rows[-6:] if len(rows) >= 6 else rows
        avg_recent = round(sum(r["new_followers"] for r in recent6) / len(recent6))
        last_month = rows[-1] if rows else None
        return {
            "best_month_label":  best["year_month"],
            "best_month_value":  best["new_followers"],
            "recent_6m_avg":     avg_recent,
            "last_month_label":  last_month["year_month"] if last_month else None,
            "last_month_value":  last_month["new_followers"] if last_month else None,
        }
    except Exception as e:
        print(f"      Warning: monthly CSV read failed — {e}")
        return {}


# ── View velocity snapshot tracking ──────────────────────────────────────────

def upsert_snapshot(snapshots: dict, post: dict) -> dict:
    """
    Records a timestamped snapshot of a post's metrics each time the script runs.
    Deduplicates snapshots within 1 hour. Prunes posts older than 90 days.
    """
    post_id   = post["id"]
    now_ts    = datetime.now(timezone.utc).timestamp()
    hours     = round((now_ts - post["date_ts"]) / 3600, 1)
    snap      = {
        "hours":    hours,
        "views":    post.get("views"),
        "likes":    post.get("likes"),
        "comments": post.get("comments"),
        "shares":   post.get("shares"),
        "ts":       int(now_ts),
    }

    if post_id not in snapshots:
        snapshots[post_id] = []

    # Don't add if we already have a snapshot within 1 hour of this one
    if not any(abs(s["hours"] - hours) < 1.0 for s in snapshots[post_id]):
        snapshots[post_id].append(snap)
        snapshots[post_id].sort(key=lambda x: x["hours"])

    # Prune posts older than 90 days
    cutoff_ts = now_ts - 90 * 86400
    for pid in list(snapshots.keys()):
        valid = [s for s in snapshots[pid] if s["ts"] > cutoff_ts]
        if valid:
            snapshots[pid] = valid
        else:
            del snapshots[pid]

    return snapshots


def get_view_velocity(snapshots: dict, post_id: str) -> dict:
    """
    Returns view counts at named checkpoints (1h, 12h, 24h, 48h, 72h) by
    interpolating from available snapshots, plus a list of all data points
    for the growth chart.
    """
    snaps = snapshots.get(post_id, [])
    if not snaps:
        return {"checkpoints": {}, "series": []}

    checkpoints = {}
    for target_h, label in [(1, "1h"), (12, "12h"), (24, "24h"), (48, "48h"), (72, "72h")]:
        # Find the closest snapshot at or after the target hour
        candidates = [s for s in snaps if s["hours"] >= target_h - 0.5]
        if candidates:
            closest = min(candidates, key=lambda s: abs(s["hours"] - target_h))
            if abs(closest["hours"] - target_h) <= 6:   # within 6h window
                checkpoints[label] = {
                    "views": closest["views"],
                    "hours": closest["hours"],
                }

    # Series for growth chart — include all snapshots up to 96h
    series = [
        {"hours": s["hours"], "views": s["views"]}
        for s in snaps
        if s["views"] is not None and s["hours"] <= 96
    ]

    return {"checkpoints": checkpoints, "series": series}


def upsert_post_history(history: list, post: dict) -> list:
    cutoff = datetime.now(timezone.utc).timestamp() - 90 * 86400
    history = [h for h in history if h.get("date_ts", 0) > cutoff and h["id"] != post["id"]]
    hours_since_post = (datetime.now(timezone.utc).timestamp() - post["date_ts"]) / 3600
    history.append({
        "id":               post["id"],
        "url":              post["url"],
        "caption":          post["caption"][:80],
        "date":             post["date"],
        "date_ts":          post["date_ts"],
        "likes":            post["likes"],
        "comments":         post["comments"],
        "views":            post.get("views"),
        "engagement":       post.get("engagement"),
        "sentiment_score":  post.get("sentiment_score", 0.5),
        "hours_since_post": round(hours_since_post, 1),
        "run_at":           datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    })
    history.sort(key=lambda x: x["date_ts"], reverse=True)
    return history


def log_followers(follower_log: list, followers) -> list:
    if followers is None:
        return follower_log
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    follower_log = [f for f in follower_log if f["date"] != today]
    follower_log.append({"date": today, "followers": followers})
    follower_log.sort(key=lambda x: x["date"])
    return follower_log[-90:]


def _median(values: list):
    vals = sorted(v for v in values if v is not None)
    if not vals: return None
    m = len(vals) // 2
    return round((vals[m] + vals[~m]) / 2) if len(vals) % 2 == 0 else vals[m]


def _percentile_rank(value, values: list) -> int:
    vals = [v for v in values if v is not None]
    if not vals or value is None: return None
    below = sum(1 for v in vals if v < value)
    return round(below / len(vals) * 100)


def _rank_label(pct: int) -> tuple:
    if pct is None: return ("", "#9ca3af")
    if pct >= 80:   return ("top 20%",      "#16a34a")
    if pct >= 60:   return ("top 40%",      "#65a30d")
    if pct >= 40:   return ("mid-range",    "#d97706")
    if pct >= 20:   return ("below median", "#ea580c")
    return              ("bottom 20%",      "#dc2626")


def compute_stats(history: list, all_posts: list, latest_id: str, follower_log: list):
    prev10 = [h for h in history if h["id"] != latest_id][:HISTORY_LIMIT]
    n      = len(prev10)

    latest_hours = next(
        (h.get("hours_since_post", 999) for h in history if h["id"] == latest_id), 999
    )
    window = max(latest_hours + 6, 30)
    prev_same_age = [h for h in prev10 if h.get("hours_since_post", 999) <= window]
    comp   = prev_same_age if len(prev_same_age) >= 3 else prev10
    n_comp = len(comp)

    med_likes      = _median([p["likes"]                    for p in comp])
    med_comments   = _median([p["comments"]                 for p in comp])
    med_views      = _median([p.get("views")                for p in comp])
    med_sentiment  = _median([p.get("sentiment_score", 0.5) for p in comp])
    med_engagement = _median([p.get("engagement")           for p in comp])
    comparison_note = (f"vs {n_comp} posts at ~{int(latest_hours)}h age"
                       if len(prev_same_age) >= 3 else "vs all tracked posts")

    best_post = max(prev10, key=lambda p: p["likes"]) if prev10 else None

    if len(all_posts) >= 2:
        newest_ts = all_posts[0]["date_ts"]
        oldest_ts = all_posts[-1]["date_ts"]
        weeks     = (newest_ts - oldest_ts) / (7 * 86400)
        posts_per_week = round(len(all_posts) / max(weeks, 0.1), 1)
    else:
        posts_per_week = None

    days_since = int((datetime.now(timezone.utc).timestamp() - all_posts[0]["date_ts"]) / 86400) if all_posts else None

    follower_growth_7d  = None
    follower_growth_30d = None
    if len(follower_log) >= 2:
        today_val = follower_log[-1]["followers"]
        cutoff_7  = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
        past_7    = [f for f in follower_log if f["date"] <= cutoff_7]
        if past_7:
            follower_growth_7d = today_val - past_7[-1]["followers"]
        cutoff_30 = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        past_30   = [f for f in follower_log if f["date"] <= cutoff_30]
        if past_30:
            follower_growth_30d = today_val - past_30[-1]["followers"]

    return {
        "prev10":             list(reversed(prev10)),
        "comp":               comp,
        "n":                  n,
        "n_comp":             n_comp,
        "med_likes":          med_likes,
        "med_comments":       med_comments,
        "med_views":          med_views,
        "med_sentiment":      med_sentiment,
        "med_engagement":     med_engagement,
        "comparison_note":    comparison_note,
        "best_post":          best_post,
        "posts_per_week":     posts_per_week,
        "days_since":         days_since,
        "follower_growth_7d": follower_growth_7d,
        "follower_growth_30d":follower_growth_30d,
        "follower_log":       follower_log[-30:],
        "latest_hours":       latest_hours,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# GROWTH TARGET TRACKER
# ═══════════════════════════════════════════════════════════════════════════════

def compute_growth_targets(follower_log: list, current_followers, monthly_csv: dict) -> dict:
    result = {
        "daily_growth":      None,
        "weekly_gained":     None,
        "weekly_target":     WEEKLY_TARGET,
        "weekly_remaining":  None,
        "weekly_days_left":  None,
        "weekly_pct":        None,
        "weekly_projected":  None,
        "monthly_gained":    None,
        "monthly_target":    MONTHLY_TARGET,
        "monthly_remaining": None,
        "monthly_days_left": None,
        "monthly_pct":       None,
        "monthly_projected": None,
        "best_month_label":  monthly_csv.get("best_month_label"),
        "best_month_value":  monthly_csv.get("best_month_value"),
        "recent_6m_avg":     monthly_csv.get("recent_6m_avg"),
        "last_month_label":  monthly_csv.get("last_month_label"),
        "last_month_value":  monthly_csv.get("last_month_value"),
    }

    if not follower_log or current_followers is None:
        return result

    today         = datetime.now(timezone.utc).date()
    yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    yesterday_entry = next((f for f in follower_log if f["date"] == yesterday_str), None)
    if yesterday_entry:
        result["daily_growth"] = current_followers - yesterday_entry["followers"]

    days_since_monday = today.weekday()
    week_start        = today - timedelta(days=days_since_monday)
    week_start_str    = week_start.strftime("%Y-%m-%d")
    pre_week  = [f for f in follower_log if f["date"] <  week_start_str]
    on_week   = [f for f in follower_log if f["date"] == week_start_str]
    week_base = on_week[0] if on_week else (pre_week[-1] if pre_week else None)

    if week_base:
        weekly_gained    = current_followers - week_base["followers"]
        weekly_remaining = max(0, WEEKLY_TARGET - weekly_gained)
        weekly_days_left = 6 - days_since_monday
        weekly_pct       = min(100, round(weekly_gained / WEEKLY_TARGET * 100, 1))
        days_elapsed_w   = days_since_monday + 1
        daily_pace_w     = weekly_gained / days_elapsed_w if days_elapsed_w else 0
        result.update({
            "weekly_gained":    weekly_gained,
            "weekly_remaining": weekly_remaining,
            "weekly_days_left": weekly_days_left,
            "weekly_pct":       weekly_pct,
            "weekly_projected": round(daily_pace_w * 7),
        })

    month_start     = today.replace(day=1)
    month_start_str = month_start.strftime("%Y-%m-%d")
    days_in_month   = calendar.monthrange(today.year, today.month)[1]
    days_elapsed_m  = today.day
    days_left_m     = days_in_month - today.day
    pre_month  = [f for f in follower_log if f["date"] <  month_start_str]
    on_month   = [f for f in follower_log if f["date"] == month_start_str]
    month_base = on_month[0] if on_month else (pre_month[-1] if pre_month else None)

    if month_base:
        monthly_gained  = current_followers - month_base["followers"]
        daily_pace_m    = monthly_gained / days_elapsed_m if days_elapsed_m else 0
        result.update({
            "monthly_gained":    monthly_gained,
            "monthly_remaining": max(0, MONTHLY_TARGET - monthly_gained),
            "monthly_days_left": days_left_m,
            "monthly_pct":       min(100, round(monthly_gained / MONTHLY_TARGET * 100, 1)),
            "monthly_projected": round(daily_pace_m * days_in_month),
        })

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL BUILDER HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def fmt(n, fallback="—"):
    if n is None: return fallback
    if isinstance(n, float):
        if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
        if n >= 1_000:     return f"{n/1_000:.1f}K"
        return f"{n:.2f}"
    n = int(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return str(n)


def _fmt_mini(n):
    if n is None: return "—"
    n = int(n)
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.0f}K"
    return str(n)


def delta_pill(val, avg, invert=False):
    if avg is None or avg == 0 or val is None: return ""
    d     = (val - avg) / avg * 100
    good  = d >= 0 if not invert else d < 0
    bg    = "#dcfce7" if good else "#fee2e2"
    color = "#166534" if good else "#991b1b"
    arrow = "▲" if d >= 0 else "▼"
    return (f'<span style="background:{bg};color:{color};font-size:11px;padding:2px 8px;'
            f'border-radius:20px;margin-left:6px;font-weight:600;">{arrow} {abs(d):.0f}%</span>')


def growth_pill(val):
    if val is None: return ""
    good  = val >= 0
    bg    = "#dcfce7" if good else "#fee2e2"
    color = "#166534" if good else "#991b1b"
    sign  = "+" if val >= 0 else ""
    return (f'<span style="background:{bg};color:{color};font-size:12px;padding:3px 9px;'
            f'border-radius:20px;font-weight:600;">{sign}{val:,}</span>')


def html_bar_chart(posts_data: list, metric: str, color: str, avg_val, label: str, suffix="") -> str:
    vals = [p.get(metric) or 0 for p in posts_data]
    if not vals or max(vals) == 0:
        return ""
    max_val = max(vals)
    n       = len(vals)
    BAR_H   = 60
    BAR_W   = 42

    cells_val = cells_bar = cells_idx = ""
    for i, v in enumerate(vals):
        bar_px     = max(4, int(v / max_val * BAR_H))
        space_px   = BAR_H - bar_px
        is_last    = (i == n - 1)
        bar_color  = "#1f2937" if is_last else color
        val_color  = "#1f2937" if is_last else "#9ca3af"
        val_weight = "bold" if is_last else "normal"
        cells_val += (f'<td width="{BAR_W}" align="center" valign="bottom" '
                      f'style="padding:0 2px 4px;font-size:9px;color:{val_color};'
                      f'font-weight:{val_weight};">{_fmt_mini(v) if v else ""}</td>')
        cells_bar += (f'<td width="{BAR_W}" align="center" valign="bottom" style="padding:0 2px;">'
                      f'<table width="{BAR_W-4}" cellpadding="0" cellspacing="0" border="0">'
                      f'<tr><td height="{space_px}" style="font-size:0;line-height:0;">&nbsp;</td></tr>'
                      f'<tr><td height="{bar_px}" bgcolor="{bar_color}" '
                      f'style="font-size:0;line-height:0;border-radius:3px 3px 0 0;">&nbsp;</td></tr>'
                      f'</table></td>')
        idx_color  = "#1f2937" if is_last else "#d1d5db"
        cells_idx += (f'<td width="{BAR_W}" align="center" '
                      f'style="padding:3px 2px 0;font-size:9px;color:{idx_color};">{i+1}</td>')

    avg_str = f"Median: {_fmt_mini(avg_val)}{suffix}" if avg_val is not None else ""
    return (f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:18px;">'
            f'<tr><td style="padding-bottom:6px;">'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td style="font-size:11px;font-weight:bold;color:#374151;'
            f'text-transform:uppercase;letter-spacing:0.05em;">{label}</td>'
            f'<td align="right" style="font-size:11px;color:#9ca3af;">{avg_str}</td>'
            f'</tr></table></td></tr>'
            f'<tr><td><table cellpadding="0" cellspacing="0" border="0">'
            f'<tr>{cells_val}</tr><tr>{cells_bar}</tr><tr>{cells_idx}</tr>'
            f'</table></td></tr></table>')


def line_chart_svg(values, color, height=48, width=200) -> str:
    if not values or len(values) < 2:
        return ""
    mn, mx = min(values), max(values)
    rng    = mx - mn or 1
    pts    = []
    for i, v in enumerate(values):
        x = int(i / (len(values) - 1) * (width - 4)) + 2
        y = height - int((v - mn) / rng * (height - 4)) - 2
        pts.append(f"{x},{y}")
    polyline = " ".join(pts)
    return (f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">'
            f'<polyline points="{polyline}" fill="none" stroke="{color}" '
            f'stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>'
            f'</svg>')


def sentiment_bar_html(score: float, analysis: dict = None) -> str:
    pct       = int(score * 100)
    empty_pct = 100 - pct
    color     = "#22c55e" if pct >= 70 else "#f59e0b" if pct >= 45 else "#ef4444"
    label     = "Positive" if pct >= 70 else "Mixed" if pct >= 45 else "Negative"
    return (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:10px 0;">'
        f'<tr><td align="right" style="font-size:12px;color:{color};font-weight:600;'
        f'padding-bottom:4px;">{pct}% {label}</td></tr>'
        f'<tr><td>'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
        f'<td width="{pct}%" height="6" bgcolor="{color}" '
        f'style="border-radius:3px 0 0 3px;font-size:0;line-height:0;">&nbsp;</td>'
        f'<td width="{empty_pct}%" height="6" bgcolor="#e5e7eb" '
        f'style="border-radius:0 3px 3px 0;font-size:0;line-height:0;">&nbsp;</td>'
        f'</tr></table></td></tr></table>'
    )


def theme_tags(themes, bg, fg):
    if not themes: return ""
    tags = "".join(
        f'<span style="background:{bg};color:{fg};padding:3px 9px;border-radius:20px;'
        f'font-size:11px;margin:2px;display:inline-block;">{t}</span>'
        for t in themes
    )
    return f'<div style="margin:6px 0;">{tags}</div>'


def stat_cell(label, value, median=None, prev_vals=None, suffix=""):
    pill = ""
    if median and value is not None and not isinstance(value, str):
        d     = (value - median) / median * 100
        good  = d >= 0
        bg    = "#dcfce7" if good else "#fee2e2"
        col   = "#166534" if good else "#991b1b"
        arrow = "▲" if good else "▼"
        pill  = (f'<span style="background:{bg};color:{col};font-size:10px;padding:1px 6px;'
                 f'border-radius:20px;margin-left:4px;font-weight:600;">{arrow}{abs(d):.0f}%</span>')

    rank_html = ""
    if prev_vals and value is not None and not isinstance(value, str):
        pct = _percentile_rank(value, prev_vals)
        lbl, col = _rank_label(pct)
        if lbl:
            rank_html = (f'<div style="font-size:10px;color:{col};font-weight:600;margin-top:2px;">'
                         f'{lbl}</div>')

    med_line = (f'<div style="font-size:10px;color:#9ca3af;margin-top:1px;">median {fmt(median)}{suffix}</div>'
                if median is not None else "")

    val_str = fmt(value) if not isinstance(value, str) else value
    return (f'<td style="text-align:center;padding:12px 10px;border-right:1px solid #f3f4f6;vertical-align:top;">'
            f'<div style="font-size:22px;font-weight:700;color:#111;line-height:1.1;">{val_str}{suffix}{pill}</div>'
            f'<div style="font-size:11px;color:#6b7280;margin-top:3px;">{label}</div>'
            f'{med_line}{rank_html}</td>')


# ─────────────────────────────────────────────────────────────────────────────
# GROWTH TARGET SECTION
# ─────────────────────────────────────────────────────────────────────────────

def build_target_section(targets: dict) -> str:
    def progress_bar_html(pct, color):
        pct = max(0, min(100, pct or 0))
        empty_pct = 100 - pct
        if pct > 0:
            return (f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:6px 0 4px;">'
                    f'<tr><td width="{pct}%" height="8" bgcolor="{color}" '
                    f'style="border-radius:4px 0 0 4px;font-size:0;line-height:0;">&nbsp;</td>'
                    f'<td width="{empty_pct}%" height="8" bgcolor="#e5e7eb" '
                    f'style="border-radius:0 4px 4px 0;font-size:0;line-height:0;">&nbsp;</td>'
                    f'</tr></table>')
        return (f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:6px 0 4px;">'
                f'<tr><td height="8" bgcolor="#e5e7eb" '
                f'style="border-radius:4px;font-size:0;line-height:0;">&nbsp;</td></tr></table>')

    def status_color(pct):
        if pct is None: return "#9ca3af"
        if pct >= 80:   return "#22c55e"
        if pct >= 40:   return "#f59e0b"
        return "#ef4444"

    def signed(n):
        if n is None: return "—"
        return f"+{n:,}" if n >= 0 else f"{n:,}"

    dg = targets.get("daily_growth")
    if dg is not None:
        dg_color   = "#22c55e" if dg >= 0 else "#ef4444"
        dg_bg      = "#dcfce7" if dg >= 0 else "#fee2e2"
        daily_pill = (f'<span style="background:{dg_bg};color:{dg_color};font-size:12px;'
                      f'font-weight:700;padding:3px 10px;border-radius:20px;">{signed(dg)} today</span>')
    else:
        daily_pill = '<span style="font-size:11px;color:#9ca3af;">First run — no daily delta yet</span>'

    wg = targets.get("weekly_gained"); wpct = targets.get("weekly_pct") or 0
    wrem = targets.get("weekly_remaining"); wlft = targets.get("weekly_days_left")
    wprj = targets.get("weekly_projected"); wclr = status_color(wpct)

    if wg is not None:
        w_proj_color = "#22c55e" if (wprj or 0) >= WEEKLY_TARGET else "#ef4444"
        w_proj_label = "on track ✓" if (wprj or 0) >= WEEKLY_TARGET else "behind pace"
        weekly_content = (f'<div style="font-size:24px;font-weight:700;color:{wclr};line-height:1;margin-bottom:2px;">{signed(wg)}</div>'
                          f'<div style="font-size:10px;color:#9ca3af;">of {WEEKLY_TARGET:,} &nbsp;·&nbsp; {wrem:,} to go &nbsp;·&nbsp; {wlft}d left</div>'
                          f'{progress_bar_html(wpct, wclr)}'
                          f'<div style="font-size:10px;color:{w_proj_color};font-weight:600;">Proj. {wprj:,} &nbsp;·&nbsp; {w_proj_label}</div>')
    else:
        weekly_content = '<div style="font-size:11px;color:#9ca3af;padding:8px 0;">Tracking starts next Monday reset</div>'

    mg = targets.get("monthly_gained"); mpct = targets.get("monthly_pct") or 0
    mrem = targets.get("monthly_remaining"); mlft = targets.get("monthly_days_left")
    mprj = targets.get("monthly_projected"); mclr = status_color(mpct)

    if mg is not None:
        m_proj_color = "#22c55e" if (mprj or 0) >= MONTHLY_TARGET else "#ef4444"
        m_proj_label = "on track ✓" if (mprj or 0) >= MONTHLY_TARGET else "behind pace"
        monthly_content = (f'<div style="font-size:24px;font-weight:700;color:{mclr};line-height:1;margin-bottom:2px;">{signed(mg)}</div>'
                           f'<div style="font-size:10px;color:#9ca3af;">of {MONTHLY_TARGET:,} &nbsp;·&nbsp; {mrem:,} to go &nbsp;·&nbsp; {mlft}d left</div>'
                           f'{progress_bar_html(mpct, mclr)}'
                           f'<div style="font-size:10px;color:{m_proj_color};font-weight:600;">Proj. {mprj:,} &nbsp;·&nbsp; {m_proj_label}</div>')
    else:
        monthly_content = '<div style="font-size:11px;color:#9ca3af;padding:8px 0;">Tracking starts next month reset</div>'

    best_v = targets.get("best_month_value"); best_l = targets.get("best_month_label", "")
    avg_6m = targets.get("recent_6m_avg");   last_v = targets.get("last_month_value")
    last_l = targets.get("last_month_label", "")

    context_cells = ""
    if best_v:
        context_cells += (f'<td align="center" style="padding:8px 12px;border-right:1px solid #f3f4f6;">'
                          f'<div style="font-size:13px;font-weight:700;color:#111;">{fmt(best_v)}</div>'
                          f'<div style="font-size:10px;color:#9ca3af;">Best ever ({best_l})</div></td>')
    if avg_6m:
        avg_color = "#22c55e" if avg_6m >= MONTHLY_TARGET else "#f59e0b"
        context_cells += (f'<td align="center" style="padding:8px 12px;border-right:1px solid #f3f4f6;">'
                          f'<div style="font-size:13px;font-weight:700;color:{avg_color};">{fmt(avg_6m)}</div>'
                          f'<div style="font-size:10px;color:#9ca3af;">6-month avg</div></td>')
    if last_v:
        last_color = "#22c55e" if last_v >= MONTHLY_TARGET else "#ef4444"
        context_cells += (f'<td align="center" style="padding:8px 12px;">'
                          f'<div style="font-size:13px;font-weight:700;color:{last_color};">{fmt(last_v)}</div>'
                          f'<div style="font-size:10px;color:#9ca3af;">Last month ({last_l})</div></td>')

    context_strip = ""
    if context_cells:
        context_strip = (f'<tr><td style="padding:0 18px 14px;">'
                         f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
                         f'style="background:#f9fafb;border-radius:8px;border:1px solid #f3f4f6;">'
                         f'<tr>{context_cells}</tr></table></td></tr>')

    return f'''
    <table width="100%" cellpadding="0" cellspacing="0" border="0"
           style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;margin-bottom:20px;">
      <tr><td style="padding:14px 18px 10px;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
          <td style="font-size:12px;font-weight:700;color:#111;text-transform:uppercase;letter-spacing:0.06em;">
            🎯 Growth Targets
          </td>
          <td align="right">{daily_pill}</td>
        </tr></table>
      </td></tr>
      <tr><td style="padding:0 18px 14px;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
          <td width="50%" style="padding-right:12px;vertical-align:top;border-right:1px solid #f3f4f6;">
            <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;font-weight:700;margin-bottom:6px;">
              Weekly · target {fmt(WEEKLY_TARGET)}
            </div>
            {weekly_content}
          </td>
          <td width="50%" style="padding-left:12px;vertical-align:top;">
            <div style="font-size:10px;color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;font-weight:700;margin-bottom:6px;">
              Monthly · target {fmt(MONTHLY_TARGET)}
            </div>
            {monthly_content}
          </td>
        </tr></table>
      </td></tr>
      {context_strip}
    </table>'''


def _build_perf_explainer_block(perf: dict) -> str:
    if not perf: return ""
    bg          = perf.get("bg", "#f3f4f6")
    fg          = perf.get("fg", "#374151")
    label       = perf.get("label", "")
    explanation = perf.get("explanation", "")
    signals     = perf.get("signals", [])
    signal_tags = "".join(
        f'<span style="background:rgba(0,0,0,0.06);color:{fg};padding:2px 8px;border-radius:20px;'
        f'font-size:10px;margin:2px;display:inline-block;">{s}</span>'
        for s in signals
    )
    signal_row = f'<div style="margin:6px 0 0;">{signal_tags}</div>' if signals else ""
    return f'''
    <div style="background:{bg};border-radius:10px;padding:12px 14px;margin:10px 0;">
      <div style="font-size:11px;font-weight:700;color:{fg};text-transform:uppercase;
                  letter-spacing:0.06em;margin-bottom:6px;">
        🔍 Why this post {("worked" if label in ("Viral 🔥", "Above average") else "performed this way")}
      </div>
      <div style="font-size:12px;font-weight:700;color:{fg};margin-bottom:4px;">{label}</div>
      <div style="font-size:12px;color:{fg};line-height:1.5;">{explanation}</div>
      {signal_row}
    </div>'''


# ─────────────────────────────────────────────────────────────────────────────
# VIDEO ANALYSIS EMAIL BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def _build_video_analysis_block(va: dict) -> str:
    """Renders the Gemini video analysis section in the email."""
    if not va:
        return ""

    def score_color(s):
        if s is None: return "#9ca3af"
        if s >= 8:    return "#22c55e"
        if s >= 6:    return "#f59e0b"
        return "#ef4444"

    def rating_color(r):
        good = {"good", "effective", "strong"}
        bad  = {"too fast", "too slow", "missing", "overwhelming", "weak"}
        if str(r).lower() in good: return "#22c55e"
        if str(r).lower() in bad:  return "#ef4444"
        return "#f59e0b"

    def score_pill(s, label=""):
        if s is None: return ""
        c = score_color(s)
        return (f'<span style="background:{c}22;color:{c};font-size:11px;font-weight:700;'
                f'padding:2px 8px;border-radius:20px;">{s}/10{" " + label if label else ""}</span>')

    def rating_pill(r):
        if not r: return ""
        c = rating_color(r)
        return (f'<span style="background:{c}22;color:{c};font-size:11px;font-weight:700;'
                f'padding:2px 8px;border-radius:20px;">{r}</span>')

    # ── Metric rows ───────────────────────────────────────────────────────────
    hook     = va.get("hook", {})
    pacing   = va.get("pacing", {})
    visuals  = va.get("visuals", {})
    overlays = va.get("text_overlays", {})
    audio    = va.get("audio", {})
    cta      = va.get("cta", {})
    overall  = va.get("overall_score")
    recs     = va.get("recommendations", [])
    summary  = va.get("summary", "")

    def metric_row(emoji, label, pill_html, note):
        return (f'<tr>'
                f'<td style="padding:7px 0;font-size:12px;color:#374151;vertical-align:top;width:30%;">'
                f'{emoji} <b>{label}</b></td>'
                f'<td style="padding:7px 0 7px 8px;vertical-align:top;">'
                f'{pill_html}'
                f'<div style="font-size:11px;color:#6b7280;margin-top:3px;">{note}</div>'
                f'</td></tr>')

    rows = ""
    if hook:
        hook_note = hook.get("note", "")
        if hook.get("duration_seconds"):
            hook_note += f' (hook lands at {hook["duration_seconds"]}s)'
        rows += metric_row("🎣", "Hook",          score_pill(hook.get("score")),          hook_note)
    if pacing:
        rows += metric_row("⚡", "Pacing",        rating_pill(pacing.get("rating")),      pacing.get("note", ""))
    if visuals:
        rows += metric_row("🎥", "Visuals",       score_pill(visuals.get("score")),       visuals.get("note", ""))
    if overlays:
        rows += metric_row("✍️", "Text overlays", rating_pill(overlays.get("rating")),   overlays.get("note", ""))
    if audio:
        rows += metric_row("🔊", "Audio",         rating_pill(audio.get("rating")),      audio.get("note", ""))
    if cta:
        cta_pill = rating_pill("present" if cta.get("present") else "missing")
        rows += metric_row("📣", "CTA",           cta_pill,                              cta.get("note", ""))

    # ── Recommendations ───────────────────────────────────────────────────────
    recs_html = ""
    if recs:
        items = "".join(
            f'<tr><td style="padding:4px 0;font-size:12px;color:#374151;">'
            f'<span style="color:#6366f1;font-weight:700;margin-right:6px;">{i+1}.</span>{r}</td></tr>'
            for i, r in enumerate(recs)
        )
        recs_html = (f'<div style="margin-top:12px;">'
                     f'<div style="font-size:11px;font-weight:700;color:#374151;'
                     f'text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">'
                     f'Recommendations</div>'
                     f'<table width="100%" cellpadding="0" cellspacing="0" border="0">'
                     f'{items}</table></div>')

    overall_bar = ""
    if overall is not None:
        pct       = int(overall / 10 * 100)
        empty_pct = 100 - pct
        oc        = score_color(overall)
        overall_bar = (
            f'<div style="margin:12px 0 4px;">'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:4px;"><tr>'
            f'<td style="font-size:11px;font-weight:700;color:#374151;">Overall score</td>'
            f'<td align="right" style="font-size:13px;font-weight:700;color:{oc};">{overall}/10</td>'
            f'</tr></table>'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td width="{pct}%" height="8" bgcolor="{oc}" '
            f'style="border-radius:4px 0 0 4px;font-size:0;line-height:0;">&nbsp;</td>'
            f'<td width="{empty_pct}%" height="8" bgcolor="#e5e7eb" '
            f'style="border-radius:0 4px 4px 0;font-size:0;line-height:0;">&nbsp;</td>'
            f'</tr></table></div>'
        )

    summary_html = (f'<div style="font-size:12px;color:#6b7280;font-style:italic;margin-top:10px;'
                    f'border-left:3px solid #6366f1;padding-left:10px;">{summary}</div>'
                    if summary else "")

    return f'''
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;
                padding:0;margin-bottom:20px;overflow:hidden;">
      <div style="background:#6366f1;padding:11px 18px;">
        <span style="color:#fff;font-size:14px;font-weight:700;">🎬 Reel Analysis</span>
        <span style="color:rgba(255,255,255,0.7);font-size:11px;margin-left:8px;">powered by Gemini 2.5 Pro</span>
      </div>
      <div style="padding:16px 18px;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          {rows}
        </table>
        {overall_bar}
        {summary_html}
        {recs_html}
      </div>
    </div>'''


# ═══════════════════════════════════════════════════════════════════════════════
# REDDIT MENTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _is_relevant(text: str) -> bool:
    """Returns True only if text contains at least one brand/name term."""
    t = text.lower()
    return any(term in t for term in REDDIT_RELEVANCE_TERMS)


def _fetch_reddit_posts(lookback_hours: int = 48) -> list:
    """Fetches Reddit posts mentioning Food Pharmer via the public JSON API."""
    cutoff   = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    seen_ids = set()
    results  = []
    for keyword in REDDIT_KEYWORDS:
        try:
            r = requests.get(
                REDDIT_BASE,
                params={"q": f'"{keyword}"', "sort": "new", "type": "link",
                        "limit": 50, "t": "week"},
                headers=REDDIT_HEADERS, timeout=15,
            )
            r.raise_for_status()
            for post in r.json().get("data", {}).get("children", []):
                p       = post["data"]
                created = datetime.fromtimestamp(p["created_utc"], tz=timezone.utc)
                if created < cutoff or p["id"] in seen_ids:
                    continue
                # Hard relevance check — title or body must mention the brand
                combined = (p["title"] + " " + (p.get("selftext") or ""))
                if not _is_relevant(combined):
                    continue
                seen_ids.add(p["id"])
                results.append({
                    "id":        p["id"],
                    "title":     p["title"],
                    "subreddit": p.get("subreddit") or p.get("subreddit_name_prefixed", "").lstrip("r/") or "—",
                    "score":     p["score"],
                    "comments":  p["num_comments"],
                    "url":       f"https://reddit.com{p['permalink']}",
                    "selftext":  (p.get("selftext") or "")[:400],
                    "created":   created.strftime("%Y-%m-%d %H:%M UTC"),
                })
        except Exception as e:
            print(f"      ⚠ Reddit post fetch failed for '{keyword}': {e}")
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def _fetch_reddit_comments(lookback_hours: int = 48) -> list:
    """Fetches Reddit comments mentioning Food Pharmer via the public JSON API."""
    cutoff  = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    seen    = set()
    results = []
    for keyword in REDDIT_KEYWORDS:
        try:
            r = requests.get(
                REDDIT_BASE,
                params={"q": f'"{keyword}"', "sort": "new", "type": "comment",
                        "limit": 25, "t": "week"},
                headers=REDDIT_HEADERS, timeout=15,
            )
            r.raise_for_status()
            for item in r.json().get("data", {}).get("children", []):
                c       = item["data"]
                created = datetime.fromtimestamp(c.get("created_utc", 0), tz=timezone.utc)
                body    = (c.get("body") or "")
                if created < cutoff or c.get("id", "") in seen:
                    continue
                if not _is_relevant(body):
                    continue
                seen.add(c["id"])
                results.append({
                    "subreddit": c.get("subreddit") or "—",
                    "body":      body[:400],
                    "score":     c.get("score", 0),
                    "url":       f"https://reddit.com{c.get('permalink', '')}",
                    "created":   created.strftime("%Y-%m-%d %H:%M UTC"),
                })
        except Exception as e:
            print(f"      ⚠ Reddit comment fetch failed for '{keyword}': {e}")
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def _summarise_reddit(posts: list, comments: list) -> dict | None:
    """Sends Reddit data to Gemini and returns a structured JSON digest."""
    if not posts and not comments:
        return None
    lines = []
    if posts:
        lines.append("=== POSTS ===")
        for p in posts[:20]:
            lines.append(f"[r/{p['subreddit']}] {p['title']} | Score:{p['score']} | {p['url']}")
            if p["selftext"]:
                lines.append(f"  > {p['selftext'][:200]}")
    if comments:
        lines.append("=== COMMENTS ===")
        for c in comments[:20]:
            lines.append(f"[r/{c['subreddit']}] Score:{c['score']} | {c['url']}\n  > {c['body']}")

    prompt = (
        'You are a media monitoring assistant for Revant Himatsingka aka "Food Pharmer", '
        "a popular Indian food/health influencer. He exposes misleading food labels and calls "
        "out harmful ingredients. Requests for him to investigate a product = PRAISE (trust signal).\n\n"
        "Analyse the Reddit data below and return ONLY valid JSON — no markdown, no fences.\n\n"
        "Schema:\n"
        '{"stats":{"total_posts":<int>,"total_comments":<int>,'
        '"most_active_subreddit":"<str>","overall_sentiment":"Positive|Negative|Neutral|Mixed"},'
        '"highlights":[{"title":"<str>","summary":"<1 sentence>","url":"<str>","subreddit":"<str>","score":<int>}],'
        '"praise":["<1 sentence each — support / requests for review = praise>"],'
        '"criticism":["<1 sentence each — negative product experiences, health warnings, controversies>"],'
        '"trending_topics":["<topic>"]}\n\n'
        "RULES: User asking Food Pharmer to investigate = praise. Bad product experience = criticism. "
        "Err on side of flagging criticism. A thread can appear in both.\n\n"
        "REDDIT DATA:\n" + "\n".join(lines)
    )
    try:
        raw = _gemini_text(prompt, max_tokens=1000, temperature=0.2)
        return json.loads(raw)
    except Exception as e:
        print(f"      ⚠ Reddit summarise failed: {type(e).__name__}: {e} — showing raw posts")
        # Fallback: show raw posts without AI analysis
        highlights = [
            {"title": p["title"], "summary": p["selftext"][:100] if p.get("selftext") else "",
             "url": p["url"], "subreddit": p["subreddit"], "score": p["score"]}
            for p in posts[:6]
        ]
        return {
            "stats": {"total_posts": len(posts), "total_comments": len(comments),
                      "most_active_subreddit": posts[0]["subreddit"] if posts else "—",
                      "overall_sentiment": "—"},
            "highlights": highlights,
            "praise": [], "criticism": [], "trending_topics": [],
        }


def _build_reddit_block(digest: dict) -> str:
    """
    Renders the Reddit mention digest as a Gmail-safe table-based section
    at the bottom of the Instagram report email.
    """
    if not digest:
        return ""

    s           = digest.get("stats", {})
    highlights  = digest.get("highlights", [])
    praise      = digest.get("praise", [])
    criticism   = digest.get("criticism", [])
    topics      = digest.get("trending_topics", [])
    sentiment   = s.get("overall_sentiment", "Neutral")
    sent_color  = {"Positive": "#16a34a", "Negative": "#dc2626",
                   "Mixed": "#d97706", "Neutral": "#6b7280"}.get(sentiment, "#6b7280")

    # ── Stats row ─────────────────────────────────────────────────────────────
    def _stat(val, label):
        return (f'<td align="center" style="padding:10px 12px;border-right:1px solid #f3f4f6;">'
                f'<div style="font-size:18px;font-weight:700;color:#111;">{val}</div>'
                f'<div style="font-size:10px;color:#9ca3af;text-transform:uppercase;'
                f'letter-spacing:0.05em;margin-top:2px;">{label}</div></td>')

    stats_row = (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
        f'style="background:#f9fafb;border-radius:8px;margin-bottom:14px;">'
        f'<tr>'
        f'{_stat(s.get("total_posts", 0), "posts")}'
        f'{_stat(s.get("total_comments", 0), "comments")}'
        f'<td align="center" style="padding:10px 12px;border-right:1px solid #f3f4f6;">'
        f'<div style="font-size:13px;font-weight:700;color:#e05c00;">'
        f'r/{s.get("most_active_subreddit") or "—"}</div>'
        f'<div style="font-size:10px;color:#9ca3af;text-transform:uppercase;'
        f'letter-spacing:0.05em;margin-top:2px;">top sub</div></td>'
        f'<td align="center" style="padding:10px 12px;">'
        f'<div style="font-size:13px;font-weight:700;color:{sent_color};">{sentiment}</div>'
        f'<div style="font-size:10px;color:#9ca3af;text-transform:uppercase;'
        f'letter-spacing:0.05em;margin-top:2px;">sentiment</div></td>'
        f'</tr></table>'
    )

    # ── Highlights ────────────────────────────────────────────────────────────
    hl_rows = ""
    for h in highlights[:6]:
        hl_rows += (
            f'<tr style="border-bottom:1px solid #f3f4f6;">'
            f'<td style="padding:8px 0;font-size:12px;vertical-align:top;">'
            f'<a href="{h.get("url","#")}" style="color:#e05c00;text-decoration:none;font-weight:500;">'
            f'{h.get("title","")[:80]}</a>'
            f'<div style="font-size:11px;color:#9ca3af;margin-top:2px;">{h.get("summary","")}</div>'
            f'</td>'
            f'<td style="padding:8px 0 8px 10px;white-space:nowrap;font-size:11px;color:#9ca3af;'
            f'vertical-align:top;">r/{h.get("subreddit","")}</td>'
            f'<td style="padding:8px 0 8px 6px;text-align:right;white-space:nowrap;'
            f'font-size:11px;color:#6b7280;vertical-align:top;">↑{h.get("score",0)}</td>'
            f'</tr>'
        )
    highlights_html = ""
    if hl_rows:
        highlights_html = (
            f'<div style="margin-bottom:14px;">'
            f'<div style="font-size:10px;font-weight:700;color:#374151;text-transform:uppercase;'
            f'letter-spacing:0.06em;margin-bottom:6px;">Top Highlights</div>'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0">{hl_rows}</table>'
            f'</div>'
        )

    # ── Praise / Criticism two-column table ───────────────────────────────────
    pc_html = ""
    if praise or criticism:
        max_rows = max(len(praise), len(criticism))
        pc_rows  = ""
        for i in range(max_rows):
            p_txt = f'✓ {praise[i]}'    if i < len(praise)    else ""
            c_txt = f'⚠ {criticism[i]}' if i < len(criticism) else ""
            pc_rows += (
                f'<tr style="border-bottom:1px solid #f9fafb;">'
                f'<td style="padding:7px 8px;font-size:11px;color:#166534;vertical-align:top;'
                f'width:50%;border-right:1px solid #f3f4f6;">{p_txt}</td>'
                f'<td style="padding:7px 8px;font-size:11px;color:#991b1b;vertical-align:top;'
                f'width:50%;">{c_txt}</td>'
                f'</tr>'
            )
        pc_html = (
            f'<div style="margin-bottom:14px;">'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="border:1px solid #f3f4f6;border-radius:8px;overflow:hidden;">'
            f'<tr style="background:#f9fafb;">'
            f'<td style="padding:7px 8px;font-size:10px;font-weight:700;color:#166534;'
            f'text-transform:uppercase;letter-spacing:0.05em;width:50%;'
            f'border-right:1px solid #f3f4f6;">Praise</td>'
            f'<td style="padding:7px 8px;font-size:10px;font-weight:700;color:#991b1b;'
            f'text-transform:uppercase;letter-spacing:0.05em;width:50%;">Criticism / Watch out</td>'
            f'</tr>'
            f'{pc_rows}</table></div>'
        )

    # ── Trending topics ───────────────────────────────────────────────────────
    topics_html = ""
    if topics:
        pills = "".join(
            f'<span style="display:inline-block;background:#fff7ed;color:#c2410c;border-radius:20px;'
            f'padding:3px 10px;font-size:11px;margin:2px 3px 2px 0;">{t}</span>'
            for t in topics
        )
        topics_html = (
            f'<div style="margin-bottom:4px;">'
            f'<div style="font-size:10px;font-weight:700;color:#374151;text-transform:uppercase;'
            f'letter-spacing:0.06em;margin-bottom:6px;">Trending on Reddit</div>'
            f'{pills}</div>'
        )

    return f'''
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;
                padding:0;margin-bottom:20px;overflow:hidden;">
      <div style="background:#e05c00;padding:11px 18px;">
        <span style="color:#fff;font-size:14px;font-weight:700;">🔴 Reddit Mentions</span>
        <span style="color:rgba(255,255,255,0.7);font-size:11px;margin-left:8px;">
          last 48 hours · Food Pharmer / Revant
        </span>
      </div>
      <div style="padding:14px 18px;">
        {stats_row}
        {highlights_html}
        {pc_html}
        {topics_html}
      </div>
    </div>'''


# ─────────────────────────────────────────────────────────────────────────────
# VIEW VELOCITY EMAIL BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def _build_view_velocity_block(velocity: dict, shares) -> str:
    """
    Renders view checkpoints (1h / 12h / 24h / 48h / 72h) and a bar chart of
    view growth over time. Also shows share count if available.
    All layout is table-based for Gmail compatibility.
    """
    if not velocity:
        return ""
    checkpoints = velocity.get("checkpoints", {})
    series      = velocity.get("series", [])

    # Need at least one checkpoint OR a share count to be worth showing
    if not checkpoints and shares is None:
        return ""

    # ── Checkpoint cells ─────────────────────────────────────────────────────
    checkpoint_labels = [("1h", "1 hour"), ("12h", "12 hours"), ("24h", "24 hours"),
                         ("48h", "48 hours"), ("72h", "72 hours")]
    cp_cells = ""
    for key, _ in checkpoint_labels:
        cp = checkpoints.get(key)
        if cp and cp.get("views") is not None:
            views_str = fmt(cp["views"])
            hours_str = f"at {cp['hours']:.0f}h"
            cp_cells += (f'<td align="center" style="padding:8px 6px;border-right:1px solid #f3f4f6;">'
                         f'<div style="font-size:14px;font-weight:700;color:#111;">{views_str}</div>'
                         f'<div style="font-size:9px;color:#9ca3af;margin-top:2px;">{key} views</div>'
                         f'<div style="font-size:9px;color:#d1d5db;">{hours_str}</div></td>')

    shares_cell = ""
    if shares is not None:
        shares_cell = (f'<td align="center" style="padding:8px 10px;">'
                       f'<div style="font-size:14px;font-weight:700;color:#6366f1;">{fmt(shares)}</div>'
                       f'<div style="font-size:9px;color:#9ca3af;margin-top:2px;">shares</div></td>')

    if not cp_cells and not shares_cell:
        return ""

    header_row = (f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
                  f'style="background:#f9fafb;border-radius:8px;margin-bottom:12px;">'
                  f'<tr>{cp_cells}{shares_cell}</tr></table>')

    # ── View growth bar chart ─────────────────────────────────────────────────
    chart_html = ""
    if len(series) >= 2:
        max_views = max(s["views"] for s in series if s["views"]) or 1
        BAR_H = 52
        BAR_W = 28
        cells_val = cells_bar = cells_lbl = ""
        for s in series:
            v       = s["views"] or 0
            h       = s["hours"]
            bar_px  = max(3, int(v / max_views * BAR_H))
            space_px= BAR_H - bar_px
            lbl     = f"{int(h)}h"
            cells_val += (f'<td width="{BAR_W}" align="center" valign="bottom" '
                          f'style="padding:0 1px 3px;font-size:8px;color:#9ca3af;">{_fmt_mini(v)}</td>')
            cells_bar += (f'<td width="{BAR_W}" align="center" valign="bottom" style="padding:0 1px;">'
                          f'<table width="{BAR_W-2}" cellpadding="0" cellspacing="0" border="0">'
                          f'<tr><td height="{space_px}" style="font-size:0;line-height:0;">&nbsp;</td></tr>'
                          f'<tr><td height="{bar_px}" bgcolor="#6366f1" '
                          f'style="font-size:0;line-height:0;border-radius:2px 2px 0 0;">&nbsp;</td></tr>'
                          f'</table></td>')
            cells_lbl += (f'<td width="{BAR_W}" align="center" '
                          f'style="padding:3px 1px 0;font-size:8px;color:#d1d5db;">{lbl}</td>')

        chart_html = (
            f'<div style="font-size:10px;font-weight:700;color:#374151;text-transform:uppercase;'
            f'letter-spacing:0.05em;margin-bottom:6px;">View growth over time</div>'
            f'<table cellpadding="0" cellspacing="0" border="0" style="margin-bottom:4px;">'
            f'<tr>{cells_val}</tr><tr>{cells_bar}</tr><tr>{cells_lbl}</tr></table>'
            f'<div style="font-size:9px;color:#d1d5db;">Each bar = one daily snapshot</div>'
        )

    note = ""
    if not checkpoints:
        note = ('<div style="font-size:11px;color:#9ca3af;font-style:italic;padding:4px 0;">'
                'Velocity checkpoints will populate as daily snapshots accumulate.</div>')

    return f'''
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;
                padding:0;margin-bottom:20px;overflow:hidden;">
      <div style="background:#6366f1;padding:11px 18px;">
        <span style="color:#fff;font-size:14px;font-weight:700;">📈 View Velocity</span>
        <span style="color:rgba(255,255,255,0.7);font-size:11px;margin-left:8px;">
          views at key milestones
        </span>
      </div>
      <div style="padding:14px 18px;">
        {header_row}
        {note}
        {chart_html}
      </div>
    </div>'''


# ─────────────────────────────────────────────────────────────────────────────
# PREVIOUS POST COMPARISON EMAIL BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def _build_prev_video_comparison_block(latest: dict, prev_post: dict) -> str:
    """
    Side-by-side comparison of latest post vs previous post.
    Shows delta pills for likes, views, comments, engagement, shares.
    """
    if not prev_post:
        return ""

    def _delta_pill(curr, prev):
        if curr is None or prev is None or prev == 0:
            return ""
        d     = (curr - prev) / prev * 100
        good  = d >= 0
        bg    = "#dcfce7" if good else "#fee2e2"
        col   = "#166534" if good else "#991b1b"
        arrow = "▲" if good else "▼"
        return (f'<span style="background:{bg};color:{col};font-size:10px;font-weight:700;'
                f'padding:1px 6px;border-radius:20px;display:inline-block;margin-top:3px;">'
                f'{arrow} {abs(d):.0f}%</span>')

    def _row(label, curr_val, prev_val, suffix=""):
        curr_str  = (fmt(curr_val) + suffix) if curr_val is not None else "—"
        prev_str  = (fmt(prev_val) + suffix) if prev_val is not None else "—"
        pill_html = _delta_pill(curr_val, prev_val)
        return (f'<tr style="border-bottom:1px solid #f3f4f6;">'
                f'<td style="padding:7px 10px;font-size:11px;color:#6b7280;width:28%;">{label}</td>'
                f'<td style="padding:7px 10px;font-size:13px;font-weight:700;color:#111;text-align:center;width:36%;">'
                f'{curr_str}<br>{pill_html}</td>'
                f'<td style="padding:7px 10px;font-size:13px;font-weight:700;color:#9ca3af;text-align:center;width:36%;">'
                f'{prev_str}</td></tr>')

    l_date  = latest.get("date", "—")
    p_date  = prev_post.get("date", "—")
    l_type  = "Reel" if latest.get("is_video") else "Photo"
    p_type  = "Reel" if prev_post.get("is_video") else "Photo"
    l_cap   = (latest.get("caption") or "")[:60].strip()
    p_cap   = (prev_post.get("caption") or "")[:60].strip()

    rows = ""
    rows += _row("Likes",      latest.get("likes"),       prev_post.get("likes"))
    rows += _row("Views",      latest.get("views"),       prev_post.get("views"))
    rows += _row("Comments",   latest.get("comments"),    prev_post.get("comments"))
    rows += _row("Engagement", latest.get("engagement"),  prev_post.get("engagement"), suffix="%")
    if latest.get("shares") is not None or prev_post.get("shares") is not None:
        rows += _row("Shares",  latest.get("shares"),  prev_post.get("shares"))

    return f'''
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;
                padding:0;margin-bottom:20px;overflow:hidden;">
      <div style="background:#374151;padding:11px 18px;">
        <span style="color:#fff;font-size:14px;font-weight:700;">↔ Post Comparison</span>
        <span style="color:rgba(255,255,255,0.6);font-size:11px;margin-left:8px;">latest vs previous</span>
      </div>
      <div style="padding:0 18px 14px;">
        <!-- Column headers -->
        <table width="100%" cellpadding="0" cellspacing="0" border="0"
               style="border-bottom:2px solid #f3f4f6;">
          <tr>
            <td style="padding:10px 10px 6px;font-size:10px;color:#9ca3af;width:28%;"></td>
            <td style="padding:10px 10px 6px;text-align:center;width:36%;">
              <div style="font-size:11px;font-weight:700;color:#E1306C;">Latest</div>
              <div style="font-size:10px;color:#9ca3af;">{l_date} · {l_type}</div>
              <div style="font-size:10px;color:#6b7280;font-style:italic;">"{l_cap}…"</div>
            </td>
            <td style="padding:10px 10px 6px;text-align:center;width:36%;">
              <div style="font-size:11px;font-weight:700;color:#6b7280;">Previous</div>
              <div style="font-size:10px;color:#9ca3af;">{p_date} · {p_type}</div>
              <div style="font-size:10px;color:#6b7280;font-style:italic;">"{p_cap}…"</div>
            </td>
          </tr>
        </table>
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          {rows}
        </table>
        <div style="padding:8px 10px 0;">
          <a href="{prev_post.get('url','#')}"
             style="font-size:11px;color:#6b7280;text-decoration:none;">
            View previous post on Instagram ↗
          </a>
        </div>
      </div>
    </div>'''


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EMAIL BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_email(post: dict, stats: dict, targets: dict,
                view_velocity: dict = None,
                prev_post: dict = None,
                reddit_digest: dict = None,
                video_analysis: dict = None) -> str:
    today    = datetime.now().strftime("%A, %d %b %Y")
    analysis = post.get("analysis", {})
    sent     = post.get("sentiment_score", 0.5)
    followers= post.get("followers")
    prev10   = stats["prev10"]
    n        = stats["n"]

    target_section = build_target_section(targets)

    fl = stats["follower_log"]
    follower_spark = ""
    if len(fl) >= 3:
        follower_spark = line_chart_svg([f["followers"] for f in fl], "#E1306C", height=40, width=160)

    follower_section = ""
    if followers:
        g7  = stats.get("follower_growth_7d")
        g30 = stats.get("follower_growth_30d")
        spark_cell = (f'<td align="right" valign="middle">{follower_spark}'
                      f'<div style="font-size:9px;color:#9ca3af;text-align:center;">30d trend</div></td>'
                      if follower_spark else "")
        follower_section = (
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0" '
            f'style="background:#fff0f5;border:1px solid #fce7f3;border-radius:12px;margin-bottom:20px;">'
            f'<tr><td style="padding:16px 20px;">'
            f'<div style="font-size:11px;color:#9d174d;text-transform:uppercase;'
            f'letter-spacing:0.06em;font-weight:700;margin-bottom:10px;">Followers</div>'
            f'<table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>'
            f'<td valign="middle" style="padding-right:20px;">'
            f'<div style="font-size:30px;font-weight:700;color:#111;line-height:1;">{fmt(followers)}</div>'
            f'<div style="font-size:12px;color:#6b7280;margin-top:3px;">total followers</div></td>'
            f'<td valign="middle" align="center" style="padding-right:20px;">'
            f'<div style="font-size:20px;font-weight:700;color:{"#22c55e" if (g7 or 0)>=0 else "#ef4444"};">'
            f'{("+" if (g7 or 0)>=0 else "") + str(g7 if g7 is not None else "—")}</div>'
            f'<div style="font-size:11px;color:#9ca3af;margin-top:2px;">7-day growth</div></td>'
            f'<td valign="middle" align="center" style="padding-right:20px;">'
            f'<div style="font-size:20px;font-weight:700;color:{"#22c55e" if (g30 or 0)>=0 else "#ef4444"};">'
            f'{("+" if (g30 or 0)>=0 else "") + str(g30 if g30 is not None else "—")}</div>'
            f'<div style="font-size:11px;color:#9ca3af;margin-top:2px;">30-day growth</div></td>'
            f'{spark_cell}</tr></table></td></tr></table>'
        )

    ppw          = stats.get("posts_per_week")
    dsince       = stats.get("days_since", 0)
    dsince_color = "#ef4444" if dsince and dsince > 5 else "#22c55e"
    eng_str      = f'{post.get("engagement"):.2f}%' if post.get("engagement") else "—"

    quick_stats = (
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:20px;"><tr>'
        f'<td width="25%" style="padding:0 5px 0 0;">'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;">'
        f'<tr><td align="center" style="padding:12px 8px;">'
        f'<div style="font-size:20px;font-weight:700;color:{dsince_color};">{dsince}d</div>'
        f'<div style="font-size:10px;color:#6b7280;margin-top:3px;">since last post</div>'
        f'</td></tr></table></td>'
        f'<td width="25%" style="padding:0 5px;">'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;">'
        f'<tr><td align="center" style="padding:12px 8px;">'
        f'<div style="font-size:20px;font-weight:700;color:#111;">{ppw if ppw else "—"}/wk</div>'
        f'<div style="font-size:10px;color:#6b7280;margin-top:3px;">posts/week</div>'
        f'</td></tr></table></td>'
        f'<td width="25%" style="padding:0 5px;">'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;">'
        f'<tr><td align="center" style="padding:12px 8px;">'
        f'<div style="font-size:20px;font-weight:700;color:#111;">{eng_str}</div>'
        f'<div style="font-size:10px;color:#6b7280;margin-top:3px;">engagement rate</div>'
        f'</td></tr></table></td>'
        f'<td width="25%" style="padding:0 0 0 5px;">'
        f'<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:10px;">'
        f'<tr><td align="center" style="padding:12px 8px;">'
        f'<div style="font-size:20px;font-weight:700;color:#111;">{n}</div>'
        f'<div style="font-size:10px;color:#6b7280;margin-top:3px;">posts tracked</div>'
        f'</td></tr></table></td>'
        f'</tr></table>'
    )

    post_age_h = stats.get("latest_hours", 0)
    age_str    = f"{int(post_age_h)}h ago" if post_age_h < 24 else f"{int(post_age_h/24)}d ago"
    comp_note  = stats.get("comparison_note", "")

    thumb_html = ""
    if post.get("thumb"):
        thumb_html = (f'<div style="text-align:center;margin-bottom:14px;">'
                      f'<img src="{post["thumb"]}" width="260" height="260" '
                      f'style="border-radius:10px;object-fit:cover;display:inline-block;">'
                      f'</div>')

    comp       = stats.get("comp", prev10)
    views_cell = stat_cell("Views",    post.get("views"),       stats.get("med_views"),
                           [p.get("views") for p in comp]) if post.get("views") else ""
    eng_cell   = stat_cell("Eng. Rate", post.get("engagement"), stats.get("med_engagement"),
                           [p.get("engagement") for p in comp], suffix="%") if post.get("engagement") else ""

    pos_tags = theme_tags(analysis.get("positive", []), "#dcfce7", "#166534")
    neg_tags = theme_tags(analysis.get("negative", []), "#fee2e2", "#991b1b")
    neu_tags = theme_tags(analysis.get("neutral",  []), "#f3f4f6", "#374151")
    summary  = (f'<div style="font-size:12px;color:#6b7280;font-style:italic;margin-top:8px;'
                f'border-left:3px solid #fce7f3;padding-left:10px;">{analysis["summary"]}</div>'
                if analysis.get("summary") else "")

    latest_block = f'''
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;
                margin-bottom:20px;overflow:hidden;">
      <div style="background:#E1306C;padding:11px 18px;">
        <span style="color:#fff;font-size:14px;font-weight:700;">Latest post</span>
        <span style="color:rgba(255,255,255,0.75);font-size:12px;margin-left:8px;">{post["date"]}</span>
        <span style="color:rgba(255,255,255,0.6);font-size:11px;margin-left:8px;">{"Reel/Video" if post.get("is_video") else "Photo"}</span>
      </div>
      <div style="padding:16px 18px;">
        {thumb_html}
        <div style="font-size:11px;color:#9ca3af;text-align:center;margin-bottom:12px;">
          {age_str} &nbsp;·&nbsp; {comp_note}
        </div>
        <table style="width:100%;border-collapse:collapse;background:#f9fafb;
                      border-radius:10px;overflow:hidden;margin-bottom:12px;">
          <tr>
            {stat_cell("Likes",    post["likes"],    stats.get("med_likes"),    [p["likes"]    for p in comp])}
            {stat_cell("Comments", post["comments"], stats.get("med_comments"), [p["comments"] for p in comp])}
            {views_cell}
            {eng_cell}
          </tr>
        </table>
        {sentiment_bar_html(sent, analysis)}
        {pos_tags}{neg_tags}{neu_tags}
        {summary}
        <div style="margin-top:10px;">
          <a href="{post["url"]}" style="font-size:12px;color:#E1306C;
                                         text-decoration:none;font-weight:500;">
            View on Instagram ↗
          </a>
        </div>
      </div>
    </div>'''

    # ── View velocity + shares block ───────────────────────────────────────────
    velocity_block = _build_view_velocity_block(view_velocity, post.get("shares"))

    # ── Previous post comparison block ────────────────────────────────────────
    comparison_block = _build_prev_video_comparison_block(post, prev_post)

    # ── Gemini video analysis block ───────────────────────────────────────────
    video_analysis_block = _build_video_analysis_block(video_analysis) if video_analysis else ""

    # ── Reddit digest block ───────────────────────────────────────────────────
    reddit_block = _build_reddit_block(reddit_digest)

    # ── X digest block ────────────────────────────────────────────────────────

    best_block = ""
    bp = stats.get("best_post")
    if bp:
        best_block = f'''
    <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:12px;
                padding:14px 18px;margin-bottom:20px;">
      <div style="font-size:12px;color:#92400e;text-transform:uppercase;
                  letter-spacing:0.06em;font-weight:600;margin-bottom:8px;">
        ⭐ Best post of last {n}
      </div>
      <div style="font-size:13px;color:#374151;font-style:italic;margin-bottom:8px;">
        "{bp.get("caption", "")[:120]}"
      </div>
      <table width="100%" cellpadding="0" cellspacing="0" border="0"><tr>
        <td style="font-size:13px;color:#6b7280;padding-right:16px;">❤️ <b style="color:#111;">{fmt(bp["likes"])}</b> likes</td>
        <td style="font-size:13px;color:#6b7280;padding-right:16px;">💬 <b style="color:#111;">{fmt(bp["comments"])}</b> comments</td>
        <td style="font-size:13px;color:#6b7280;padding-right:16px;">📅 {bp["date"]}</td>
        <td><a href="{bp["url"]}" style="color:#E1306C;text-decoration:none;font-size:12px;">View ↗</a></td>
      </tr></table>
    </div>'''

    if prev10:
        has_views = any(p.get("views")      for p in prev10)
        has_eng   = any(p.get("engagement") for p in prev10)
        likes_chart    = html_bar_chart(prev10, "likes",      "#f472b6", stats.get("med_likes"),      "Likes")
        comments_chart = html_bar_chart(prev10, "comments",   "#818cf8", stats.get("med_comments"),   "Comments")
        views_chart    = html_bar_chart(prev10, "views",      "#fb923c", stats.get("med_views"),      "Views")       if has_views else ""
        eng_chart      = html_bar_chart(prev10, "engagement", "#34d399", stats.get("med_engagement"), "Engagement", "%") if has_eng else ""
        charts_block = f'''
    <div style="background:#fff;border:1px solid #e5e7eb;border-radius:12px;
                padding:18px 20px;margin-bottom:20px;">
      <div style="font-size:13px;font-weight:600;color:#111;margin-bottom:16px;">
        Last {n} posts — performance trend
      </div>
      {likes_chart}{comments_chart}{views_chart}{eng_chart}
      <div style="font-size:11px;color:#9ca3af;border-top:1px solid #f3f4f6;padding-top:8px;">
        1 = oldest · {n} = latest (dark bar)
      </div>
    </div>'''
    else:
        charts_block = '<div style="font-size:12px;color:#9ca3af;padding:10px 0;text-align:center;">Trend charts appear after a few daily runs.</div>'

    return f'''<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
  <div style="max-width:640px;margin:0 auto;padding:28px 16px;">

    <!-- Header -->
    <div style="text-align:center;margin-bottom:24px;">
      <div style="font-size:13px;color:#E1306C;font-weight:600;
                  text-transform:uppercase;letter-spacing:0.08em;margin-bottom:4px;">
        Instagram Daily Report
      </div>
      <h1 style="font-size:24px;font-weight:700;color:#111;margin:0 0 4px;">@foodpharmer</h1>
      <p style="font-size:13px;color:#6b7280;margin:0;">{today}</p>
    </div>

    {target_section}
    {follower_section}
    {quick_stats}
    {latest_block}
    {velocity_block}
    {comparison_block}
    {best_block}
    {charts_block}
    {reddit_block}
    {video_analysis_block}

    <div style="text-align:center;padding:20px 0 8px;font-size:11px;color:#9ca3af;">
      Apify + Gemini · Data as of {datetime.now().strftime("%H:%M IST")}
    </div>
  </div>
</body>
</html>'''


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL SENDER
# ═══════════════════════════════════════════════════════════════════════════════

def send_email(html: str, subject: str):
    print(f"\n  Sending to: {', '.join(EMAIL_TO)}")
    msg            = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_USER
    msg["To"]      = ", ".join(EMAIL_TO)
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_PASS)
        server.sendmail(GMAIL_USER, EMAIL_TO, msg.as_string())
    print("  ✓ Email sent!")


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_ts(ts) -> datetime:
    if isinstance(ts, str):
        for date_fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                         "%Y-%m-%dT%H:%M:%S+00:00", "%Y-%m-%d"):
            try:
                return datetime.strptime(ts, date_fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            pass
    elif isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    return datetime.now(timezone.utc)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{'='*60}")
    print(f"  @foodpharmer Instagram Daily Report")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    # 1. Scrape
    latest, all_posts = scrape_instagram()

    # 2. Analyse comments
    print(f"\n[2/3] Analysing {len(latest.get('comment_texts', []))} comments with Gemini...")
    analysis = analyse(latest.pop("comment_texts", []), latest["caption"])
    latest["analysis"]        = analysis
    latest["sentiment_score"] = analysis.get("score", 0.5)
    print(f"  Sentiment: {latest['sentiment_score']:.0%} — {analysis.get('summary','')[:60]}")

    # 2b. Gemini video analysis
    print("\n[2b/3] Running Gemini video analysis...")
    video_analysis = analyse_video(latest)
    latest["video_analysis"] = video_analysis
    if video_analysis:
        print(f"  Video score: {video_analysis.get('overall_score')}/10 — {video_analysis.get('summary','')[:80]}")
    else:
        print("  Video analysis skipped or failed.")

    # 3. Update histories
    print("\n[3/3] Updating history & building email...")
    ig_history   = load_json(IG_HISTORY,    [])
    follower_log = load_json(FOLLOWER_LOG,   [])
    snapshots    = load_json(SNAPSHOTS_FILE, {})
    follower_log = bootstrap_follower_log_from_csv(follower_log)

    for p in all_posts:
        if p["id"] == latest["id"]:
            p["sentiment_score"] = latest["sentiment_score"]
        ig_history = upsert_post_history(ig_history, p)

    # Record a view/like snapshot for the latest post (accumulates across daily runs)
    snapshots = upsert_snapshot(snapshots, latest)

    follower_log = log_followers(follower_log, latest.get("followers"))
    save_json(IG_HISTORY,    ig_history)
    save_json(FOLLOWER_LOG,  follower_log)
    save_json(SNAPSHOTS_FILE, snapshots)
    print(f"      ✓ Snapshot saved ({len(snapshots.get(latest['id'], []))} data points for latest post)")

    # 4. Compute stats + targets
    stats       = compute_stats(ig_history, all_posts, latest["id"], follower_log)
    monthly_csv = load_monthly_csv_context()
    targets     = compute_growth_targets(follower_log, latest.get("followers"), monthly_csv)


    # 4c. View velocity
    view_velocity = get_view_velocity(snapshots, latest["id"])
    n_snaps = len(snapshots.get(latest["id"], []))
    n_cps   = len(view_velocity.get("checkpoints", {}))
    print(f"      View velocity: {n_snaps} snapshots · {n_cps} checkpoints resolved")

    # 4d. Previous post (for side-by-side comparison)
    prev_post = all_posts[1] if len(all_posts) > 1 else None
    if prev_post:
        print(f"      Comparison post: {prev_post['id']} ({prev_post['date']})")

    # 4f. Reddit mentions
    print("\n[4b/5] Fetching Reddit mentions of @foodpharmer / Revant...")
    reddit_posts    = _fetch_reddit_posts(REDDIT_LOOKBACK_HOURS)
    reddit_comments = _fetch_reddit_comments(REDDIT_LOOKBACK_HOURS)
    print(f"      {len(reddit_posts)} posts, {len(reddit_comments)} comments found")
    reddit_digest = _summarise_reddit(reddit_posts, reddit_comments)

    # 5. Build & send email
    today   = datetime.now().strftime("%d %b %Y")
    subject = f"@foodpharmer Instagram Report — {today}"
    html    = build_email(latest, stats, targets,
                          view_velocity=view_velocity,
                          prev_post=prev_post,
                          reddit_digest=reddit_digest,
                          video_analysis=latest.get("video_analysis"))

    with open("report.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("  report.html saved — open in browser to preview")

    send_email(html, subject)

    # 6. Summary
    print(f"\n{'='*60}")
    print(f"  Latest post  : {latest['id']} ({latest['date']})")
    print(f"  Likes        : {latest['likes']:,}  (median {stats['med_likes']})")
    print(f"  Comments     : {latest['comments']:,}  (median {stats['med_comments']})")
    print(f"  Followers    : {fmt(latest.get('followers'))}")
    print(f"  Sentiment    : {latest['sentiment_score']:.0%}")
    print(f"  Shares       : {latest.get('shares')}")
    print(f"  Is video     : {latest.get('is_video')}")
    print(f"  Velocity CPs : {list(view_velocity.get('checkpoints', {}).keys())}")
    print(f"  Snapshots    : {n_snaps} total")
    print(f"  Prev post    : {prev_post['id'] if prev_post else 'N/A'}")
    print(f"  Days since   : {stats['days_since']}d")
    print(f"  Posts/week   : {stats['posts_per_week']}")
    print(f"  Daily Δ      : {targets.get('daily_growth')}")
    print(f"  Weekly       : {targets.get('weekly_gained')} / {WEEKLY_TARGET:,} ({targets.get('weekly_pct')}%)")
    print(f"  Monthly      : {targets.get('monthly_gained')} / {MONTHLY_TARGET:,} ({targets.get('monthly_pct')}%)")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
