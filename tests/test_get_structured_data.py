import json
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from closet.get_structured_data import (
    enrich_data,
    load_data,
    load_enriched_data,
)


@pytest.fixture
def playlist_with_subtitles_path(test_data_dir: Path) -> Path:
    """Returns the path to the playlist with subtitles test data."""
    return test_data_dir / "playlist_with_subtitles.json"


def test_load_data(tmp_path, playlist_with_subtitles_path):
    """Test that load_data loads the data correctly."""
    # Arrange
    mock_data = [{"id": "1", "title": "video 1"}]
    data_path = tmp_path / "playlist_with_subtitles.json"
    with open(data_path, "w") as f:
        for item in mock_data:
            f.write(json.dumps(item) + "\n")

    # Act
    with patch("closet.get_structured_data.PLAYLIST_WITH_SUBTITLES_JSON_PATH", data_path):
        data = list(load_data())

    # Assert
    assert data == mock_data


def test_load_enriched_data(tmp_path):
    """Test that load_enriched_data loads and deduplicates the data correctly."""
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
    assert len(enriched_df) == 2
    assert sorted(enriched_df["id"].to_list()) == ["1", "2"]


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


@patch("closet.get_structured_data._extract_structured_data_from_record")
def test_enrich_data_partially_enriched(mock_extract, tmp_path):
    """Test that enrich_data enriches partially enriched records."""
    # Arrange
    playlist_data = [{"id": "1", "title": "video 1"}]
    enriched_df = pl.DataFrame(
        [{"id": "1", "guest": None, "movies": None}],
        schema={"id": pl.Utf8, "guest": pl.Utf8, "movies": pl.List(pl.Utf8)},
    )
    mock_extract.return_value = {"id": "1", "guest": "guest 1", "movies": ["movie 1"]}
    data_path = tmp_path / "enriched_playlist.json"
    enriched_df.write_ndjson(data_path)

    # Act
    with patch("closet.get_structured_data.ENRICHED_PLAYLIST_JSON_PATH", data_path):
        enrich_data(playlist_data, enriched_df)

    # Assert
    mock_extract.assert_called_once()
    enriched_data = pl.read_ndjson(data_path)
    assert len(enriched_data) == 1
    assert enriched_data["guest"][0] == "guest 1"
