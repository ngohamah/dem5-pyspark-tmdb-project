"""Configuration values for the TMDB Spark pipeline.

Centralizing paths, API settings, and column lists here means the rest of
the pipeline never hard-codes a literal path or constant inline.
"""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

RAW_PAYLOAD_PATH = DATA_DIR / "raw_payloads.json"
RAW_SPARK_INPUT_PATH = DATA_DIR / "raw_movies.jsonl"
SAMPLE_PAYLOAD_PATH = DATA_DIR / "sample_payloads.json"
CLEAN_DATA_PATH = DATA_DIR / "clean_movies.csv"

REPORT_PATH = BASE_DIR / "reports" / "movie_analysis_report.md"
PLOTS_DIR = BASE_DIR / "reports" / "plots"

# Replace with a real TMDB API key (https://www.themoviedb.org/settings/api).
# Left as a placeholder, the pipeline logs a warning and falls back to the
# bundled sample payloads so it still runs end-to-end offline.
TMDB_API_KEY = "YOUR_TMDB_API_KEY"
TMDB_API_URL = "https://api.themoviedb.org/3/movie/{movie_id}"
# append_to_response=credits pulls cast/crew in the same request as the
# movie details, so each movie only costs a single API call.
TMDB_API_PARAMS = {"language": "en-US", "append_to_response": "credits"}

SPARK_APP_NAME = "tmdb-movie-analysis"
SPARK_MASTER = "local[*]"

MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    420818, 24428, 168259, 99861, 284054, 12445,
    181808, 330457, 351286, 109445, 321612, 260513,
]

IRRELEVANT_COLUMNS = [
    "adult", "imdb_id", "original_title", "video", "homepage",
]
JSON_STRUCT_COLUMNS = ["belongs_to_collection"]
JSON_ARRAY_COLUMNS = [
    "genres", "production_countries", "production_companies", "spoken_languages",
]

# Matches the project spec's reorder list, plus profit_musd/roi -- both are
# derived once here (instead of being recomputed on every ranking query) since
# Step 3 rankings need them directly.
FINAL_COLUMNS = [
    "id", "title", "tagline", "release_date", "genres", "belongs_to_collection",
    "original_language", "budget_musd", "revenue_musd", "profit_musd", "roi",
    "production_companies", "production_countries", "vote_count", "vote_average",
    "popularity", "runtime", "overview", "spoken_languages", "poster_path",
    "cast", "cast_size", "director", "crew_size",
]

# Minimum number of non-null columns a row must retain to survive cleaning.
MIN_NON_NULL_COLUMNS = 10
# ROI rankings only consider reasonably well-funded productions.
MIN_BUDGET_FOR_ROI_MUSD = 10
# Rating rankings ignore movies with too few votes to be meaningful.
MIN_VOTES_FOR_RATING = 10
