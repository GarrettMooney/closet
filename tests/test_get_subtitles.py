from unittest.mock import patch


from closet.get_subtitles import (
    get_videos_to_process,
    load_enriched_playlist,
    load_playlist,
)


def test_get_videos_to_process(playlist_path, playlist_with_subtitles_path):
    """
    Tests that the get_videos_to_process function correctly identifies videos to process.
    """
    with (
        patch("closet.get_subtitles.PLAYLIST_JSON_PATH", playlist_path),
        patch(
            "closet.get_subtitles.PLAYLIST_WITH_SUBTITLES_JSON_PATH",
            playlist_with_subtitles_path,
        ),
    ):
        playlist_df = load_playlist()
        enriched_df = load_enriched_playlist()
        videos_to_process, _ = get_videos_to_process(playlist_df, enriched_df)
        assert set(videos_to_process["id"].to_list()) == {"2", "4"}
