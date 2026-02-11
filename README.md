# Monsters Among Us (MAU) Podcast — Data Scraping & Tidy Data Pipeline

## What This Does

A Python pipeline that scrapes every episode of the *Monsters Among Us* paranormal podcast (~550+ episodes across 20+ seasons), transcribes audio locally using OpenAI Whisper, extracts individual caller stories via LLM, runs sentiment analysis, and outputs a **tidy data spreadsheet** ready for statistical analysis in R or Python.

No Spotify account needed. No paid transcription services. Transcription is free and local.

### Pipeline Architecture

```
┌──────────────┐    ┌──────────────────┐    ┌────────────────┐    ┌────────────┐
│  1. FETCH    │───▶│  2. TRANSCRIBE   │───▶│   3. PARSE     │───▶│  4. EXPORT │
│  RSS feeds   │    │  Whisper (local,  │    │  LLM extracts  │    │  openpyxl  │
│  (episode    │    │  free, no API)   │    │  each caller   │    │  → .xlsx   │
│   catalog)   │    │                  │    │  + VADER senti-│    │  tidy data │
│              │    │                  │    │  ment analysis  │    │            │
└──────────────┘    └──────────────────┘    └────────────────┘    └────────────┘
  episode_list.json   transcripts/*.txt      parsed/*.json       MAU_Tidy_Data.xlsx
                      audio/*.mp3
```

---

## Setup

### Prerequisites

- **Python 3.10+**
- **FFmpeg** (required by Whisper for audio processing): https://ffmpeg.org/download.html
- **An LLM API key** — at least one of:
  - Anthropic (Claude): https://console.anthropic.com/
  - OpenAI: https://platform.openai.com/api-keys

### Installation

```bash
pip install feedparser requests openpyxl anthropic openai tqdm python-dotenv openai-whisper nltk
```

Create a `.env` file in the same directory as the script:

```ini
# Required — at least one API key:
ANTHROPIC_API_KEY=sk-ant-api03-your-key-here
OPENAI_API_KEY=sk-proj-your-key-here

# Optional — Whisper model size (default: medium)
# Options: tiny, base, small, medium, large
# "base" is fastest for testing, "medium" is recommended, "large" is most accurate
WHISPER_MODEL=medium
```

---

## Usage

### Quick Start

```bash
# Test with 3 episodes to make sure everything works
python mau_scraper_v2.py --step all --api haiku --limit 3

# Full pipeline (all ~550 episodes)
python mau_scraper_v2.py --step all --api haiku
```

### All Commands

```bash
python mau_scraper_v2.py --step fetch                         # Fetch episode list from RSS
python mau_scraper_v2.py --step transcribe                    # Download audio + transcribe
python mau_scraper_v2.py --step transcribe --limit 10         # Transcribe first 10 only
python mau_scraper_v2.py --step parse --api haiku             # Parse transcripts with Claude Haiku
python mau_scraper_v2.py --step parse --api o4-mini           # Parse with OpenAI o4-mini
python mau_scraper_v2.py --step export                        # Export to XLSX from cached data
python mau_scraper_v2.py --step reparse-regions --api haiku   # Fix encounter locations only
python mau_scraper_v2.py --step reset-parsed                  # Wipe parsed JSON (re-parse)
python mau_scraper_v2.py --step reset                         # Wipe ALL cached data
```

### Choosing an LLM

Use `--api` to select. No file editing needed.

| Flag | Model | Provider | Cost per 550 eps | Notes |
|------|-------|----------|-------------------|-------|
| `--api haiku` | Claude Haiku 4.5 | Anthropic | ~$12 | Default. Best price/performance |
| `--api sonnet` | Claude Sonnet 4 | Anthropic | ~$38 | Best quality |
| `--api o4-mini` | o4-mini | OpenAI | ~$13 | Reasoning model |
| `--api gpt-4o-mini` | GPT-4o mini | OpenAI | ~$2 | Cheapest option |
| `--api gpt-4.1-mini` | GPT-4.1 mini | OpenAI | ~$5 | Mid-range |

Each stage caches its output, so you can switch models between runs without redoing work. Use `--step reset-parsed` to clear parse cache if you want to re-parse with a different model.

---

## Output: Tidy Data

The output spreadsheet follows **Hadley Wickham's tidy data principles**:
- Each **row** = one caller's story (one observation)
- Each **column** = one variable
- Missing values are explicit `NA` (recognized natively by R and pandas)

### Column Reference (25 variables)

| Variable | Type | Description |
|----------|------|-------------|
| `instance` | int | Unique counter across all episodes |
| `season` | int / NA | Season number |
| `episode` | int / NA | Episode number within season |
| `episode_title` | string | Full episode title |
| `release_date` | date | Air date (YYYY-MM-DD) |
| `call_start_time` | string / NA | Timestamp in episode (HH:MM:SS) |
| `caller_name` | string / NA | Name as spoken, or NA if anonymous |
| `country` | string / NA | Country where encounter took place |
| `state_or_region` | string / NA | Full US state name or foreign region |
| `city` | string / NA | City of encounter if mentioned |
| `us_census_region` | categorical / NA | Auto-derived: Northeast / Southeast / Midwest / West |
| `call_type` | categorical | Primary entity from controlled taxonomy (60+ types) |
| `call_type_secondary` | categorical / NA | Secondary classification if applicable |
| `description` | string | 1–2 sentence factual summary of encounter |
| `date_of_event` | date / NA | When the encounter happened (ISO-8601) |
| `time_of_event` | string / NA | Time of day (24h HH:MM format) |
| `setting` | string / NA | Physical location (bedroom, highway, forest, etc.) |
| `involves_other_witnesses` | boolean | Did others also witness the event? |
| `caller_emotional_tone` | categorical | scared / calm / nostalgic / humorous / matter-of-fact / emotional |
| `sentiment_compound` | float / NA | VADER overall sentiment (−1.0 to +1.0) |
| `sentiment_pos` | float / NA | Proportion positive (0.0 to 1.0) |
| `sentiment_neg` | float / NA | Proportion negative (0.0 to 1.0) |
| `sentiment_neu` | float / NA | Proportion neutral (0.0 to 1.0) |
| `derek_commentary` | string / NA | Notable host reaction or explanation |
| `verified` | boolean | Hallucination check passed? |

### Entity Taxonomy (60+ types across 6 categories)

**Cryptid:** Bigfoot/Sasquatch, Dogman, Mothman, Skinwalker, Wendigo, Thunderbird, Crawler/Rake, Chupacabra, Lake Monster, Black-Eyed Kids, Not Deer, Goatman, Jersey Devil, and more.

**Ghostly/Spiritual:** Ghost/Apparition, Shadow Person, Hat Man, Old Hag, Poltergeist, Demonic Entity, Angel/Positive Spirit, Doppelganger, Sleep Paralysis Entity, and more.

**UFO/Aerial:** UFO/UAP, Black Triangle, Orb/Light, Abduction, Alien Entity, and more.

**High Strangeness:** Missing Time, Glitch in the Matrix, Time Slip, Premonition, Telepathy, Men in Black, and more.

**Environmental/Sensory:** Phantom Sound, Phantom Smell, Unexplained Animal Behavior, Electronic Malfunction, Temperature Anomaly.

**Other:** Coincidence, Dream/Vision, Curse/Hex, Fairy/Fae, and more.

---

## Features

### Hallucination Verification
After parsing each episode, every caller record is checked against the original transcript. The system looks for evidence that the caller actually exists in the transcript (name match, intro snippet match, description keywords). Results appear in the `verified` column.

### VADER Sentiment Analysis
Each caller's description is scored with VADER (Valence Aware Dictionary and sEntiment Reasoner) from NLTK. This runs locally with zero API cost and provides reproducible numeric sentiment scores alongside the LLM's categorical tone classification. The two measure different things: VADER scores the event description's word polarity, while `caller_emotional_tone` captures the LLM's interpretation of how the caller felt.

### Encounter Location Extraction
Location fields (country, state, city) specifically capture **where the encounter took place**, not where the caller currently lives. A caller who says "I live in Texas but this happened in Oregon" will have Oregon as their state. The `--step reparse-regions` command can fix locations in already-parsed data without a full reparse (~$0.50–1.50 for all episodes).

### Live Progress & Cost Estimates
Before any work begins, the script prints a time/cost estimate showing how many episodes need processing vs. how many are already cached. During parsing, a live ETA updates after each episode based on a rolling average.

### Auto-Versioned Exports
The XLSX export never overwrites previous files: `MAU_Tidy_Data.xlsx` → `MAU_Tidy_Data_v2.xlsx` → `_v3.xlsx`, etc.

### Tracker URL Cleanup
Podcast analytics redirects (arttrk.com, podtrac.com, etc.) are automatically stripped to download audio directly from the source CDN.

---

## Limitations

| Limitation | Impact | Mitigation |
|---|---|---|
| **Transcript quality** | Auto-generated transcripts contain errors in names, locations, numbers | LLM prompt handles noisy input; description captures meaning not verbatim text |
| **LLM consistency** | Same entity type may get slightly different labels across episodes | Fixed taxonomy of 60+ types in prompt; post-hoc sanitizer normalizes case and fuzzy-matches |
| **Caller boundary detection** | No consistent delimiter between callers in transcripts | LLM-based segmentation using host intros, self-intros, and topic transitions |
| **VADER on summaries** | Sentiment scores reflect event polarity, not caller emotion | Clearly documented; both measures useful for different analyses |
| **Patreon content** | Bonus "Beyond" episodes are behind a paywall | Not automatically scraped; manual transcripts can be placed in `data/transcripts/` |
| **Newer episode URLs** | Some recent episodes use tracker redirect chains that may fail | Auto-cleaned to direct CDN URLs |

---

## Cost & Runtime Estimates

| Resource | Estimate |
|----------|----------|
| Disk space | ~15 GB (audio) + ~200 MB (transcripts, cache, output) |
| LLM API cost | ~$2–38 depending on model (see table above) |
| Transcription cost | Free (Whisper runs locally) |
| Transcription time | ~5–40 hours depending on Whisper model and hardware |
| Parse time | ~30–60 minutes for all episodes |
| VADER sentiment | Milliseconds (local, no cost) |

---

## File Structure

```
mau_scraper_v2.py       # Main pipeline script
.env                    # API keys (create this — see Setup)
README.md               # This file
data/
├── episode_list.json   # Cached episode catalog from RSS
├── audio/              # Downloaded MP3 files
├── transcripts/        # One .txt + _timestamps.json per episode
├── parsed/             # One .json per episode (caller records)
└── output/
    └── MAU_Tidy_Data.xlsx  # Final tidy data output
```

---

## Analysis Ideas

The dataset enables several research directions:

1. **Geography of the Paranormal** — Choropleth maps of entity types by state/region. Do certain phenomena cluster geographically?
2. **Temporal Patterns** — When do encounters happen? Plot frequency by hour of day, faceted by entity category.
3. **Witness Effect** — Which phenomena are almost always solo vs. multi-witness? How does corroboration correlate with sentiment?
4. **Show Evolution** — Has the content shifted over 20+ seasons? Did certain entity types spike after related media (TV shows, subreddits)?
5. **Event vs. Affect** — Compare VADER sentiment scores with LLM tone classifications. The gap between event severity and caller affect could indicate emotional processing over time.

---

## License

This tool is for research and personal use. The *Monsters Among Us* podcast content belongs to its creators. This scraper processes publicly available RSS feeds and generates structured analytical data from transcripts.
