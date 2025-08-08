import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from closet.get_structured_data import (
    clean_enriched_data,
    enrich_data,
    load_enriched_data,
    load_playlist_with_subtitles,
)


@pytest.fixture
def playlist_with_subtitles_path(test_data_dir: Path) -> Path:
    """Returns the path to the playlist with subtitles test data."""
    return test_data_dir / "playlist_with_subtitles.json"


def test_load_playlist_with_subtitles(tmp_path, playlist_with_subtitles_path):
    """Test that load_playlist_with_subtitles loads the data correctly."""
    # Arrange
    mock_data = [{"id": "1", "title": "video 1"}]
    data_path = tmp_path / "playlist_with_subtitles.json"
    with open(data_path, "w") as f:
        for item in mock_data:
            f.write(json.dumps(item) + "\n")

    # Act
    with patch("closet.get_structured_data.PLAYLIST_WITH_SUBTITLES_JSON_PATH", data_path):
        data = list(load_playlist_with_subtitles())

    # Assert
    assert data == mock_data


def test_load_enriched_data(tmp_path):
    """Test that load_enriched_data loads the data correctly."""
    # Arrange
    mock_data = [
        {"id": "1", "guest": "guest 1", "movies": ["movie 1"]},
        {"id": "1", "guest": None, "movies": None},
        {"id": "2", "guest": "guest 2", "movies": ["movie 2"]},
    ]
    data_path = tmp_path / "enriched_playlist.json"
    with open(data_path, "w") as f:
        for item in mock_data:
            f.write(json.dumps(item) + "\n")

    # Act
    with patch("closet.get_structured_data.ENRICHED_PLAYLIST_JSON_PATH", data_path):
        enriched_df = load_enriched_data()

    # Assert
    assert enriched_df is not None
    assert len(enriched_df) == 3


def test_clean_enriched_data():
    """Test that clean_enriched_data deduplicates the data correctly."""
    # Arrange
    mock_data = [
        {"id": "1", "guest": "guest 1", "movies": ["movie 1"]},
        {"id": "1", "guest": None, "movies": None},
        {"id": "2", "guest": "guest 2", "movies": ["movie 2"]},
    ]
    enriched_df = pl.DataFrame(mock_data)

    # Act
    cleaned_df = clean_enriched_data(enriched_df)

    # Assert
    assert len(cleaned_df) == 2
    assert sorted(cleaned_df["id"].to_list()) == ["1", "2"]


@patch("closet.get_structured_data._extract_structured_data_from_record")
def test_enrich_data_new_records(mock_extract, tmp_path):
    """Test that enrich_data enriches new records."""
    # Arrange
    playlist_data = [{"id": "1", "title": "video 1"}]
    enriched_df = None
    mock_extract.return_value = {"id": "1", "guest": "guest 1", "movies": ["movie 1"]}
    data_path = tmp_path / "enriched_playlist.json"

    # Act
    with patch("closet.get_structured_data.ENRICHED_PLAYLIST_JSON_PATH", data_path):
        enrich_data(playlist_data, enriched_df)

    # Assert
    assert data_path.exists()
    enriched_data = pl.read_ndjson(data_path)
    assert len(enriched_data) == 1
    assert enriched_data["guest"][0] == "guest 1"


@patch("closet.get_structured_data._extract_structured_data_from_record")
def test_enrich_data_already_enriched(mock_extract, tmp_path):
    """Test that enrich_data skips already enriched records."""
    # Arrange
    playlist_data = [{"id": "1", "title": "video 1"}]
    enriched_df = pl.DataFrame(
        [{"id": "1", "guest": "guest 1", "movies": ["movie 1"]}]
    )
    data_path = tmp_path / "enriched_playlist.json"

    # Act
    with patch("closet.get_structured_data.ENRICHED_PLAYLIST_JSON_PATH", data_path):
        enrich_data(playlist_data, enriched_df)

    # Assert
    mock_extract.assert_not_called()
