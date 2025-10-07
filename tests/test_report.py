import tempfile
from pathlib import Path
from unittest.mock import patch


from closet.report import generate_report, load_data, save_report_to_markdown


def test_generate_report(enriched_playlist_path):
    """
    Tests that the generate_report function correctly calculates the report statistics.
    """
    with patch("closet.report.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        report = generate_report(df)
        assert report["total_records"] == 3
        assert report["records_with_subtitles"] == 2
        assert report["records_with_structured_data"] == 3
        assert round(report["subtitle_percentage"], 2) == 66.67
        assert round(report["structured_data_percentage"], 2) == 100.0


def test_save_report_to_markdown(enriched_playlist_path):
    """
    Tests that the save_report_to_markdown function creates a correctly formatted markdown file.
    """
    with patch("closet.report.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        report = generate_report(df)

        # Create a temporary directory for the test
        with tempfile.TemporaryDirectory() as temp_dir:
            report_path = Path(temp_dir) / "test_report.md"

            # Save the report to markdown
            save_report_to_markdown(report, report_path)

            # Verify the file was created
            assert report_path.exists(), "Report markdown file was not created"

            # Read and verify the contents
            content = report_path.read_text(encoding="utf-8")

            # Check for key elements in the markdown content
            assert "# Data Enrichment Report" in content
            assert "**Total videos:** 3" in content
            assert "**Videos with subtitles:** 2" in content
            assert "**Enrichment percentage:** 66.67%" in content
            assert "**Videos with structured data:** 3" in content
            assert "**Enrichment percentage:** 100.00%" in content
            assert "Last updated:" in content
            assert (
                "generated automatically by the Criterion Closet data pipeline"
                in content
            )
