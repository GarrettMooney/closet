import sys
from typing import Dict, Generator, Iterable, Set

import srsly

from closet.config import ENRICHED_PLAYLIST_JSON_PATH, PLAYLIST_WITH_SUBTITLES_JSON_PATH
from closet.enrich import _extract_structured_data_from_record
from closet.logging import console


def load_data() -> Generator[Dict, None, None]:
    """Loads the playlist data with subtitles.

    Yields
    ------
    Dict
        The playlist data as a generator of dictionaries.

    Raises
    ------
    FileNotFoundError
        If the playlist data file is not found.
    """
    if not PLAYLIST_WITH_SUBTITLES_JSON_PATH.exists():
        raise FileNotFoundError("No playlist with subtitles found.")
    yield from srsly.read_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH)


def load_enriched_ids() -> Set[str]:
    """Loads the IDs of already enriched records.

    Returns
    -------
    Set[str]
        A set of enriched record IDs.
    """
    if ENRICHED_PLAYLIST_JSON_PATH.exists():
        enriched_data = srsly.read_jsonl(ENRICHED_PLAYLIST_JSON_PATH)
        enriched_ids = {record["id"] for record in enriched_data}
        console.log(f"Found {len(enriched_ids)} enriched records.")
        return enriched_ids
    return set()


def enrich_data(playlist_data: Iterable[Dict], enriched_ids: Set[str]):
    """Enriches the playlist data with structured information.

    Parameters
    ----------
    playlist_data : Iterable[Dict]
        An iterator of the playlist data to enrich.
    enriched_ids : Set[str]
        A set of already enriched record IDs.
    """
    with console.status("Enriching records...") as status:
        for record in playlist_data:
            if record["id"] in enriched_ids:
                enriched_record = next(
                    (
                        r
                        for r in srsly.read_jsonl(ENRICHED_PLAYLIST_JSON_PATH)
                        if r["id"] == record["id"]
                    ),
                    None,
                )
                if (
                    enriched_record
                    and enriched_record.get("guest")
                    and enriched_record.get("movies")
                ):
                    status.console.log(f"Skipping {record['id']}: already enriched.")
                    continue

            status.update(f"Enriching {record['id']}...")
            enriched_record = _extract_structured_data_from_record(record)
            srsly.write_jsonl(
                ENRICHED_PLAYLIST_JSON_PATH, [enriched_record], append=True
            )
            enriched_ids.add(record["id"])

    console.log("Enriched playlist data has been created.")


def main():
    """Main entry point for the script."""
    playlist_data = load_data()
    enriched_ids = load_enriched_ids()
    enrich_data(playlist_data, enriched_ids)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        console.log(str(e), style="red")
        sys.exit(1)
