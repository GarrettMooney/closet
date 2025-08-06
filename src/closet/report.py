import polars as pl
import srsly
from pathlib import Path

from closet.logging import console

DATA_DIR = Path("data")
PLAYLIST_WITH_SUBTITLES_JSON_PATH = DATA_DIR / "playlist_with_subtitles.json"


def main():
    """
    Generates a report on the completeness of the subtitle data.
    """
    if not PLAYLIST_WITH_SUBTITLES_JSON_PATH.exists():
        console.log("No enriched playlist data found.", style="red")
        return

    enriched_data = list(srsly.read_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH))
    enriched_df = pl.DataFrame(enriched_data)

    total_records = len(enriched_df)
    records_with_subtitles = enriched_df.filter(
        pl.col("subtitles").is_not_null()
    ).height

    if total_records == 0:
        percentage = 0.0
    else:
        percentage = (records_with_subtitles / total_records) * 100

    console.print("\n[bold blue]Subtitle Enrichment Report[/bold blue]")
    console.print(f"Total videos: {total_records}")
    console.print(f"Videos with subtitles: {records_with_subtitles}")
    console.print(f"Enrichment percentage: {percentage:.2f}%")


if __name__ == "__main__":
    main()
