from typing import List

from mirascope import llm
from pydantic import BaseModel, Field


class Movie(BaseModel):
    """A Pydantic model for a movie."""

    title: str | None = Field(..., description="The title of the movie.")
    description: str | None = Field(
        ..., description="A brief description of the movie."
    )


class VideoInfo(BaseModel):
    """A Pydantic model for the structured video information."""

    guest: str | None = Field(..., description="The name of the guest.")
    year: str | None = Field(..., description="The year the video was published.")
    movies: List[Movie] = Field(
        ..., description="A list of movies discussed in the video."
    )


@llm.call(
    provider="google",
    model="gemini-2.5-pro",
    response_model=VideoInfo,
)
def extract_video_info(title: str, subtitles: str) -> str:
    """Extracts structured data from the title and subtitles."""
    return f"Extract the structured data from the following video transcript.\n\nTitle: {title}\n\nTranscript:\n{subtitles}"


def _extract_structured_data_from_record(record: dict) -> dict:
    """Extracts structured data from a single record.

    Parameters
    ----------
    record : dict
        A dictionary representing a single video record.

    Returns
    -------
    dict
        The record enriched with structured data.
    """
    if not record.get("subtitles"):
        return record

    try:
        structured_data = extract_video_info(record["title"], record["subtitles"])
        record.update(structured_data.model_dump())
    except Exception:
        record.update(VideoInfo(guest=None, year=None, movies=[]).model_dump())

    return record
