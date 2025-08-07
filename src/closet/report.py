import sys
from typing import Dict

import polars as pl
import srsly

from closet.config import ENRICHED_PLAYLIST_JSON_PATH
from closet.logging import console
from pathlib import Path


def load_data() -> pl.DataFrame:
    """Loads the enriched playlist data.

    Returns
    -------
    pl.DataFrame
        The enriched playlist data as a Polars DataFrame.

    Raises
    ------
    FileNotFoundError
        If the enriched playlist data file is not found.
    """
    if not ENRICHED_PLAYLIST_JSON_PATH.exists():
        raise FileNotFoundError(
            "No enriched playlist data found. Please run `uv run update-playlist` first."
        )
    return pl.DataFrame(list(srsly.read_jsonl(ENRICHED_PLAYLIST_JSON_PATH)))


def generate_report(enriched_df: pl.DataFrame) -> Dict[str, float]:
    """Generates a report on the completeness of the data.

    Parameters
    ----------
    enriched_df : pl.DataFrame
        The enriched playlist data.

    Returns
    -------
    Dict[str, float]
        A dictionary with the report data.
    """
    total_records = len(enriched_df)
    records_with_subtitles = enriched_df.filter(
        pl.col("subtitles").is_not_null()
    ).height
    subtitle_percentage = (
        (records_with_subtitles / total_records) * 100 if total_records > 0 else 0.0
    )
    records_with_structured_data = enriched_df.filter(
        (pl.col("guest") != "Unknown")
        | (pl.col("year") != "Unknown")
        | (pl.col("movies").list.len() > 0)
    ).height
    structured_data_percentage = (
        (records_with_structured_data / total_records) * 100
        if total_records > 0
        else 0.0
    )
    return {
        "total_records": total_records,
        "records_with_subtitles": records_with_subtitles,
        "subtitle_percentage": subtitle_percentage,
        "records_with_structured_data": records_with_structured_data,
        "structured_data_percentage": structured_data_percentage,
    }


def display_report(report: Dict[str, float]):
    """Displays the data enrichment report.

    Parameters
    ----------
    report : Dict[str, float]
        A dictionary with the report data.
    """
    console.print("\n[bold blue]Data Enrichment Report[/bold blue]")
    console.print(f"Total videos: {report['total_records']}")

    console.print("\n[bold green]Subtitle Enrichment[/bold green]")
    console.print(f"Videos with subtitles: {report['records_with_subtitles']}")
    console.print(f"Enrichment percentage: {report['subtitle_percentage']:.2f}%")

    console.print("\n[bold green]Structured Data Enrichment[/bold green]")
    console.print(
        f"Videos with structured data: {report['records_with_structured_data']}"
    )
    console.print(f"Enrichment percentage: {report['structured_data_percentage']:.2f}%")


def save_report_to_markdown(report: Dict[str, float], output_path: Path):
    """Saves the data enrichment report to a markdown file.

    Parameters
    ----------
    report : Dict[str, float]
        A dictionary with the report data.
    output_path : Path
        Path where the markdown file should be saved.
    """
    from datetime import datetime
    
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
    
    markdown_content = f"""# Data Enrichment Report

*Last updated: {current_time}*

## Summary

**Total videos:** {report['total_records']}

## Subtitle Enrichment

- **Videos with subtitles:** {report['records_with_subtitles']}
- **Enrichment percentage:** {report['subtitle_percentage']:.2f}%

## Structured Data Enrichment

- **Videos with structured data:** {report['records_with_structured_data']}
- **Enrichment percentage:** {report['structured_data_percentage']:.2f}%

---

*This report was generated automatically by the Criterion Closet data pipeline.*
"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(markdown_content)


def main():
    """Main entry point for the script."""
    enriched_df = load_data()
    report = generate_report(enriched_df)
    display_report(report)
    
    # Save report to markdown file
    report_path = Path("report.md")
    save_report_to_markdown(report, report_path)
    console.print(f"\n[dim]Report saved to {report_path}[/dim]")


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        console.log(str(e), style="red")
        sys.exit(1)
