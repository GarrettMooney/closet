import subprocess
from typing import Dict, List

import srsly

from closet.config import DATA_DIR, PLAYLIST_JSON_PATH, YOUTUBE_PLAYLIST_URL
from closet.logging import console


def fetch_playlist() -> List[Dict]:
    """Fetches the playlist from YouTube.

    Returns
    -------
    List[Dict]
        The playlist data as a list of dictionaries.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    command = [
        "yt-dlp",
        "--dump-json",
        "--flat-playlist",
        YOUTUBE_PLAYLIST_URL,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return [srsly.json_loads(line) for line in result.stdout.strip().split("\n")]


def save_playlist(data: List[Dict]):
    """Saves the playlist data to a file.

    Parameters
    ----------
    data : List[Dict]
        The playlist data.
    """
    srsly.write_json(PLAYLIST_JSON_PATH, data, indent=2)
    console.log("Playlist data has been fetched.")


def main():
    """Main entry point for the script."""
    playlist_data = fetch_playlist()
    save_playlist(playlist_data)


if __name__ == "__main__":
    main()
