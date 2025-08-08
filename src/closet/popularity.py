import sys
from collections import Counter
from typing import Dict, Generator, Iterable

import srsly

from closet.config import ENRICHED_PLAYLIST_JSON_PATH
from closet.logging import console


def load_data() -> Generator[Dict, None, None]:
    """Loads the enriched playlist data.

    Yields
    ------
    Dict
        The enriched playlist data as a generator of dictionaries.

    Raises
    ------
    FileNotFoundError
        If the enriched playlist data file is not found.
    """
    if not ENRICHED_PLAYLIST_JSON_PATH.exists():
        raise FileNotFoundError("Enriched playlist data not found.")
    yield from srsly.read_jsonl(ENRICHED_PLAYLIST_JSON_PATH)


def calculate_popularity(enriched_data: Iterable[Dict]) -> Counter:
    """Calculates the popularity of each film.

    Parameters
    ----------
    enriched_data : Iterable[Dict]
        An iterator of the enriched playlist data.

    Returns
    -------
    Counter
        A Counter object with the popularity of each film.
    """
    return Counter(
        (
            selection["title"]
            for record in enriched_data
            if "movies" in record
            for selection in record["movies"]
            if selection["title"]
        )
    )


def display_popularity(film_counter: Counter):
    """Displays the most popular films.

    Parameters
    ----------
    film_counter : Counter
        A Counter object with the popularity of each film.
    """
    console.log("Most popular films:")
    for film, count in film_counter.most_common(30):
        console.log(f"- [green]{film}[/green]: {count}")


def main():
    """Main entry point for the script."""
    enriched_data = load_data()
    film_counter = calculate_popularity(enriched_data)
    display_popularity(film_counter)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        console.log(str(e), style="red")
        sys.exit(1)
