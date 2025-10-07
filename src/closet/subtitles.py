import os
import re
import subprocess
import tempfile
from pathlib import Path

from closet.errors import RateLimitError, SubtitleError


def get_subtitles(video_id: str, language: str = "en", cookies_file: str = None) -> str:
    """Load YouTube video subtitles as a fragment.

    Parameters
    ----------
    video_id : str
        The ID of the YouTube video.
    language : str, optional
        The language of the subtitles to fetch, by default "en"
    cookies_file : str, optional
        Path to cookies file for authentication, by default None

    Returns
    -------
    str
        The cleaned subtitle content.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            _download_subtitles(video_id, language, temp_dir, cookies_file)
            subtitle_content = _read_subtitle_file(temp_dir)
            return _clean_vtt_content(subtitle_content)
        except subprocess.CalledProcessError as e:
            error_message = e.stderr if e.stderr else str(e)
            if "429" in error_message:
                raise RateLimitError(f"Rate limit hit for video {video_id}")
            elif "Sign in to confirm" in error_message or "not a bot" in error_message:
                raise SubtitleError(
                    f"YouTube anti-bot detection triggered for {video_id}. "
                    "Your cookies may be stale or invalid. Try refreshing them."
                )
            elif "not available on this app" in error_message.lower():
                raise SubtitleError(
                    f"Video {video_id} blocked by YouTube. "
                    "This usually means stale cookies or outdated yt-dlp. "
                    "Try: 1) Update cookies, 2) Update yt-dlp"
                )
            elif (
                "login required" in error_message.lower()
                or "members-only" in error_message.lower()
            ):
                raise SubtitleError(
                    f"Video {video_id} requires authentication. "
                    "Ensure your cookies file is from a logged-in YouTube session."
                )
            else:
                raise SubtitleError(
                    f"Failed to download subtitles for {video_id}: {error_message}"
                )
        except Exception as e:
            raise SubtitleError(f"Error processing YouTube video {video_id}: {str(e)}")


def _download_subtitles(
    video_id: str, language: str, temp_dir: str, cookies_file: str = None
):
    """Download subtitles using yt-dlp.

    Parameters
    ----------
    video_id : str
        The ID of the YouTube video.
    language : str
        The language of the subtitles to fetch.
    temp_dir : str
        The temporary directory to download the subtitles to.
    cookies_file : str, optional
        Path to cookies file for authentication.
    """
    base_cmd = [
        "yt-dlp",
        "--skip-download",
        "--sub-format",
        "vtt",
        "-o",
        f"{temp_dir}/%(id)s.%(ext)s",
        f"https://www.youtube.com/watch?v={video_id}",
    ]

    # Add cookies if provided
    if cookies_file and Path(cookies_file).exists():
        base_cmd.extend(["--cookies", cookies_file])

    # Add additional headers and options to avoid bot detection
    base_cmd.extend(
        [
            # Use Android client to avoid restrictions
            "--extractor-args",
            "youtube:player_client=android,web",
            "--extractor-args",
            "youtube:player_skip=webpage,configs",
            # Updated user agent
            "--user-agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            # Add delays to appear more human
            "--sleep-requests",
            "1",  # Sleep between requests
            "--sleep-subtitles",
            "1",  # Sleep between subtitle requests
            # Additional anti-bot options
            "--no-check-certificates",
            "--prefer-free-formats",
        ]
    )

    # Try to download manually created subtitles first
    manual_cmd = base_cmd + ["--write-sub", "--sub-lang", language]
    try:
        subprocess.run(manual_cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError:
        # If no manual subtitles are found, try auto-generated ones
        if not any(f.endswith(".vtt") for f in os.listdir(temp_dir)):
            auto_cmd = base_cmd + ["--write-auto-sub", "--sub-lang", language]
            subprocess.run(auto_cmd, check=True, capture_output=True, text=True)


def _read_subtitle_file(temp_dir: str) -> str:
    """Read the downloaded subtitle file from the temporary directory.

    Parameters
    ----------
    temp_dir : str
        The temporary directory where the subtitles were downloaded.

    Returns
    -------
    str
        The content of the subtitle file.
    """
    subtitle_files = [
        os.path.join(temp_dir, f) for f in os.listdir(temp_dir) if f.endswith(".vtt")
    ]
    if not subtitle_files:
        raise SubtitleError("No subtitle file found.")

    with open(subtitle_files[0], "r", encoding="utf-8") as f:
        return f.read()


def _clean_vtt_content(content: str) -> str:
    """Clean up the VTT subtitle content to make it more readable.

    Parameters
    ----------
    content : str
        The raw VTT subtitle content.

    Returns
    -------
    str
        The cleaned subtitle content.
    """
    lines = content.split("\n")
    cleaned_lines = []
    prev_text = None
    last_minute_recorded = -1

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if (
            not line
            or line.startswith("WEBVTT")
            or line.startswith("Kind:")
            or line.startswith("Language:")
        ):
            i += 1
            continue

        if "-->" in line:
            timestamp_match = re.match(r"(\d{2}:\d{2}:\d{2})", line)
            if timestamp_match:
                current_timestamp = timestamp_match.group(1)
                current_minute = int(current_timestamp.split(":")[1])
                if current_minute != last_minute_recorded:
                    cleaned_lines.append(f"[{current_timestamp}]")
                    last_minute_recorded = current_minute
            i += 1
            continue

        if line.isdigit():
            i += 1
            continue

        if line:
            clean_line = re.sub(r"<[^>]+>", "", line)
            if clean_line.strip() and clean_line != prev_text:
                cleaned_lines.append(clean_line)
                prev_text = clean_line
        i += 1

    return "\n".join(cleaned_lines)
