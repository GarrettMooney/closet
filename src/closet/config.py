import os
from pathlib import Path

YOUTUBE_PLAYLIST_URL = "https://www.youtube.com/playlist?list=PL7D89754A5DAD1E8E"
DATA_DIR = Path("data")
PLAYLIST_JSON_PATH = DATA_DIR / "playlist.json"
PLAYLIST_WITH_SUBTITLES_JSON_PATH = DATA_DIR / "playlist_with_subtitles.json"
ENRICHED_PLAYLIST_JSON_PATH = DATA_DIR / "enriched_playlist.json"
LOG_HTML_PATH = DATA_DIR / "log.html"
REPORT_MD_PATH = Path("report.md")
INDEX_NAME = "closet"

# Cookies configuration
COOKIES_FILE = os.getenv("YOUTUBE_COOKIES_FILE", DATA_DIR / "cookies.txt")

# Batch processing configuration
MAX_VIDEOS_PER_RUN = (
    int(os.getenv("MAX_VIDEOS_PER_RUN", "0")) or None
)  # 0 or empty means no limit
