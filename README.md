# Marvel Rivals Performance Pipeline

A end-to-end data engineering + ML pipeline built on **Databricks** that asks a simple question: *did the game actually reward you fairly?*

Marvel Rivals is a 6v6 hero shooter where your rank score (`add_score`) goes up or down based almost entirely on whether your team wins or loses — regardless of how well *you* personally performed. This project pulls real match data via API, builds a full medallion architecture pipeline, and computes a **role-weighted performance score** to surface the gap between individual contribution and rank point rewards.

Built as both a personal analytics tool and preparation for the **Databricks Data Engineer Associate certification**.

---

## The Problem

In Marvel Rivals, a support player can output 70,000+ healing per 10 minutes and still lose rank points because their teammates underperformed. The game's `add_score` system doesn't distinguish between:

- A healer who carried the team and lost vs. a healer who went AFK and lost
- A tank who absorbed massive damage vs. one who hid in the back

This pipeline builds a fairer metric — one that accounts for *what your role requires* and *how well you delivered it*.

---

## Architecture

```
Local Python (PyCharm)
└── Data ingestion via marvelrivalsapi.com
    ├── Squad match history (219 records across 6 players)
    └── Full 12-player match details (118 unique matches)

Databricks (Unity Catalog: marvel_rivals)
├── Bronze Layer — raw ingestion
│   ├── raw_match_history      (219 rows, Auto Loader from NDJSON)
│   └── raw_match_details      (118 rows, Auto Loader from JSON)
│
├── Silver Layer — cleaned & transformed
│   ├── player_match_stats     (1,416 rows — all 12 players per match, parsed)
│   ├── player_hero_stats      (1,973 rows — per player-hero, per-10 metrics)
│   ├── hero_lookup            (52 rows — hero reference table with roles)
│   └── squad_match_history    (219 rows — squad rank score context)
│
└── Gold Layer — aggregated & scored
    ├── match_performance_scores   (1,973 rows — role-weighted 0-100 score)
    └── squad_performance_summary  (5 rows — fairness gap per squad member)
```

---

## Key Concepts

### Medallion Architecture
Data flows through three layers of increasing refinement:
- **Bronze** — raw files exactly as received from the API, stored as Delta tables via Auto Loader
- **Silver** — cleaned, typed, parsed, and normalized (nested JSON → proper columns, per-10-minute metrics)
- **Gold** — aggregated, scored, and ready for dashboards and ML

### Role-Weighted Performance Score
Each hero in Marvel Rivals belongs to one of three roles. Stats are weighted differently per role:

| Role | Primary weight | Secondary weight | Penalty |
|---|---|---|---|
| **Strategist** (healer) | healing_per10 (0.40) | assists_per10 (0.25) | deaths_per10 (-0.20) |
| **Vanguard** (tank) | damage_taken_per10 (0.35) | damage_per10 (0.20) | deaths_per10 (-0.20) |
| **Duelist** (DPS) | kills_per10 (0.30) | damage_per10 (0.30) | deaths_per10 (-0.20) |

The raw weighted score is then **min-max normalized to 0-100** across the full dataset so scores are comparable across matches, heroes, and roles.

### Fairness Gap
The core metric of this project:

```
fairness_gap = avg_performance_score - avg_add_score
```

A positive fairness gap means the game is consistently under-rewarding you relative to your actual contribution. Every squad member in this dataset has a positive fairness gap.

### Per-10-Minute Metrics
All stats are normalized by play time to account for:
- Different match lengths
- Players who switched heroes mid-match
- Role comparisons (a healer with 0 kills isn't underperforming — they're doing their job)

Formula: `(raw_stat / play_time_seconds) * 600`

---

## Reading the Data

### `gold.match_performance_scores`
The main output table. One row per player-hero per match.

| Column | Description |
|---|---|
| `match_uid` | Unique match identifier |
| `player_uid` | Unique player identifier |
| `nick_name` | Player username (anonymized in public version) |
| `hero_name` | Hero played |
| `hero_role` | Strategist / Vanguard / Duelist |
| `kills_per_10_minutes` | Kills normalized per 10 minutes of play |
| `deaths_per_10_minutes` | Deaths normalized per 10 minutes |
| `assists_per_10_minutes` | Assists normalized per 10 minutes |
| `healing_per_10_minutes` | Healing output normalized per 10 minutes |
| `damage_per_10_minutes` | Damage dealt normalized per 10 minutes |
| `damage_taken_per_10_minutes` | Damage absorbed normalized per 10 minutes |
| `session_hit_rate` | Accuracy (% of shots that hit) |
| `add_score` | Rank points the game awarded/deducted |
| `raw_performance_score` | Weighted sum before normalization |
| `performance_score_0_100` | Final normalized score (0 = worst, 100 = best) |
| `role_weight_applied` | Which role weighting was used |

### `gold.squad_performance_summary`
One row per squad member. The headline fairness analysis.

| Column | Description |
|---|---|
| `canonical_name` | Anonymized player label (Player_You, Player_A, etc.) |
| `total_matches` | Total matches played across main + alt accounts |
| `avg_performance_score` | Average 0-100 score across all matches |
| `avg_add_score` | Average rank points awarded by the game |
| `fairness_gap` | The difference — how much the game under/over rewards relative to performance |
| `win_rate_pct` | Win percentage |
| `most_played_hero` | Most frequently played hero |
| `most_played_role` | Most frequently played role |

**How to read `fairness_gap`:**
- `fairness_gap > 0` → the game is under-rewarding you (your performance score is higher than your rank reward)
- `fairness_gap ≈ 0` → the game's reward roughly matches your contribution
- `fairness_gap < 0` → the game is over-rewarding you (your rank went up more than your performance justifies)

---

## Notebooks

| Notebook | Description |
|---|---|
| `notebooks/marvel_rivals.ipynb` | Local ingestion — API calls, pagination, JSON file output |
| `notebooks/marvel_rivals_heroes.ipynb` | Hero lookup table ingestion |
| `notebooks/01_bronze_ingestion.ipynb` | Auto Loader streams → Bronze Delta tables |
| `notebooks/02_silver_transformations.ipynb` | JSON parsing, exploding, per-10 metrics, Silver tables |
| `notebooks/03_gold_performance.ipynb` | Role-weighted scoring, normalization, Gold tables |

---

## Tech Stack

- **Databricks Free Edition** — notebooks, serverless SQL warehouse, Unity Catalog
- **Delta Lake** — ACID transactions, time travel, schema evolution
- **Auto Loader** (`cloudFiles`) — incremental file ingestion with checkpointing
- **Spark SQL** — all Silver and Gold transformations
- **Python / PySpark** — local ingestion pipeline, API calls
- **Databricks CLI** — bulk file upload to Unity Catalog Volumes
- **marvelrivalsapi.com** — community Marvel Rivals API (requires API key)
- **Git / GitHub** — version control, Databricks Git Folders integration

---

## Setup

### Prerequisites
- Databricks Free Edition account
- Python 3.12+ with `requests`, `python-dotenv`
- Databricks CLI installed and authenticated
- API key from marvelrivalsapi.com

### Environment Variables
Create a `.env` file at the project root (never committed to git):
```
MARVEL_RIVALS_API_KEY=your_api_key_here
```

### Local Ingestion
Run `notebooks/marvel_rivals.ipynb` in order — it will:
1. Resolve squad usernames to player UIDs
2. Pull full paginated match history per player
3. Deduplicate and pull full 12-player match details
4. Save anonymized NDJSON files locally

### Upload to Databricks
```bash
databricks fs cp marvel_rivals_data/match_history/ \
    dbfs:/Volumes/marvel_rivals/bronze/raw_files/match_history/ --recursive

databricks fs cp marvel_rivals_data/match_details/ \
    dbfs:/Volumes/marvel_rivals/bronze/raw_files/match_details/ --recursive
```

### Run Pipeline
Execute notebooks in order:
1. `01_bronze_ingestion` — creates `raw_match_history` and `raw_match_details`
2. `02_silver_transformations` — creates all Silver tables
3. `03_gold_performance` — creates `match_performance_scores` and `squad_performance_summary`

---

## Data Privacy

All squad member usernames are anonymized (`Player_You`, `Player_A` through `Player_E`) in any public-facing output. Raw Bronze data containing real usernames is stored only in private Databricks Volumes and local gitignored folders — never committed to this repository.

---

## Roadmap

- [ ] Module 3: Lakeflow Declarative Pipelines (automated pipeline orchestration)
- [ ] Module 4: Unity Catalog governance, data quality expectations
- [ ] Module 5: ML model (MLflow) — learn optimal role weights from data, predict fair `add_score`
- [ ] Module 6: AI layer — Databricks AI Functions for natural language match summaries
- [ ] Module 7: Lakeview dashboard — visual squad analytics

---

## Cert Prep Coverage

This project covers the following Databricks Data Engineer Associate exam domains:

| Domain | Coverage |
|---|---|
| Databricks Lakehouse Platform (~10%) | Unity Catalog, Delta Lake, medallion architecture |
| ELT with Spark SQL and Python (~31%) | CTEs, window functions, JSON parsing, explode, from_json |
| Incremental Data Processing (~30%) | Auto Loader, checkpointing, trigger(availableNow=True) |
| Production Pipelines (~18%) | Lakeflow Jobs (upcoming), notebook orchestration |
| Data Governance (~11%) | Unity Catalog schemas, volumes, table lineage |
