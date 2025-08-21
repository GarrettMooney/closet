class SubtitleError(Exception):
    """Base exception for subtitle-related errors."""

    pass


class RateLimitError(SubtitleError):
    """Exception raised for 429 rate-limiting errors."""

    pass


class BotDetectionError(Exception):
    """Raised when YouTube's anti-bot detection is triggered."""

    pass


class VideoUnavailableError(Exception):
    """Raised when a video is unavailable or private."""

    pass
