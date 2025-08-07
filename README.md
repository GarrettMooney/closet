# Criterion Closet

This project contains tools for interacting with the Criterion Closet YouTube playlist.
This repo is centered around building up a dataset to poke at over time with various experiments.
We will start with things that are relatively simple like counting and full-text-search, but will work our way up to trying out more novel techniques on the dataset as we get inspired by new repos or arxiv papers.

The main goal is to have a fun data sandbox to play in!

## Core Features

-   **Data Pipeline:** Fetch the playlist, enrich it with subtitles, and extract structured data.
-   **Popularity:** Find the most popular films in the Criterion Closet.
-   **Recommendations:** Get movie recommendations based on a list of titles.

## Installation

Install the project and its dependencies using `uv`:

```bash
uv pip install -e .
```

## Usage

The core features of this project are exposed through two main scripts: `popularity` and `recommend`. The data pipeline that fetches and enriches the data will be run automatically (e.g., via GitHub Actions).

### Find Popular Films

To see a list of the most popular movies, run:

```bash
uv run popularity
```

This will output a list of the most frequently selected films in the Criterion Closet.

### Get Recommendations

To get movie recommendations based on one or more films, run:

```bash
uv run recommend "The Red Shoes, The 400 Blows"
```

This will provide a list of recommended films based on the co-occurrence of the films you provided.

## Experimental Features

This project also includes WIP experimental features, such as a Redis-backed search index and a FastAPI and Svelte frontend. For more information, please see the [experimental features documentation](src/closet/experimental/README.md).

## Data Completeness

The project automatically generates a data enrichment report as part of the GitHub Actions pipeline that runs daily. You can always find the latest report in the `report.md` file at the root of this repository.

The report includes:
- Total number of videos in the dataset
- Percentage of videos with subtitles
- Percentage of videos with structured data (guest, year, movies)

### Manual Report Generation

To generate a report locally during development, run:

```bash
uv run report
```

This will display the report in your terminal and update the `report.md` file.

## Automated Data Pipeline

The project uses GitHub Actions to automatically maintain and enrich the dataset daily. The pipeline:

1. Fetches new videos from the Criterion Closet playlist
2. Downloads subtitles for new videos
3. Extracts structured data using an LLM
4. Generates an updated data completeness report
5. Commits changes only when meaningful updates occur

The pipeline is configured to run daily at midnight UTC and can also be triggered manually from the GitHub Actions tab.
