import sys

from closet.get_new_videos import main as get_new_videos
from closet.get_subtitles import main as get_subtitles
from closet.get_structured_data import main as get_structured_data
from closet.logging import console


def main():
    """Main entry point for the data processing pipeline."""
    try:
        console.log("Starting the data processing pipeline...")

        console.log("Step 1: Fetching new videos...")
        get_new_videos()

        console.log("Step 2: Fetching subtitles...")
        get_subtitles()

        console.log("Step 3: Enriching data with structured information...")
        get_structured_data()

        console.log("Pipeline finished successfully.", style="bold green")
    except Exception as e:
        console.log(f"An error occurred during the pipeline execution: {e}", style="bold red")
        sys.exit(1)


if __name__ == "__main__":
    main()
