<div align="center">

# Closet

*A dataset for exploring the Criterion Closet picks*

</div>

---

## What is this?

The **Closet** is repo for updating the dataset built from [the YouTube series](https://www.youtube.com/playlist?list=PL7D89754A5DAD1E8E) where filmmakers pick their favorite films from the Criterion Collection. 

This project automatically collects, enriches, and analyzes every episode to help you discover patterns, popular picks, and (try to) get personalized film recommendations.

This repo is centered around building up a dataset to poke at over time with various experiments.
We will start with things that are relatively simple like counting and full-text-search, but will work our way up to trying out more novel techniques on the dataset as we get inspired by new repos or arxiv papers.

## 🔄 How it Works

Our automated pipeline runs weekly, transforming raw playlist data into rich, structured insights:

```
📺 Fetch Videos → 📝 Extract Subtitles → 🤖 LLM Analysis → 📊 Generate Report on Completeness
```

- **📺 Data Collection**: Automatically discovers new videos from the Criterion Closet playlist
- **📝 Content Enrichment**: Downloads subtitles, metadata, and uses LLMs to extract film mentions
- **📊 Reporting**: Generates data completeness reports and tracks enrichment progress

## 🚀 Quick Start

Install the project using `uv`:

```bash
uv pip install -e .
```

## 💡 Usage

### Find the Most Popular Films

Discover which films are mentioned most frequently across all episodes:

```bash
uv run popularity
```

### Get Personalized Recommendations

Get film suggestions based on your favorite picks:

```bash
uv run recommend "The Red Shoes"
```

You can also provide multiple films as a comma-separated string:

```bash
uv run recommend "The Red Shoes, The 400 Blows"
```

### Generate a Data Report

Check the health and completeness of your dataset:

```bash
uv run report
```

**NB:** This is run as part of the automated pipeline, but you can run it manually to see the latest stats.

## 📊 Live Data

The pipeline automatically maintains fresh data and generates reports. Check out [`report.md`](report.md) for the latest statistics on dataset completeness, including subtitle coverage and structured data enrichment percentages.

## 🧪 Experimental Features

We're always experimenting! 

Current work-in-progress features include:

- **Redis-backed search index** for lightning-fast film lookups
- **FastAPI + Svelte frontend** for interactive data exploration

[→ Learn more about experimental features](src/closet/experimental/README.md)

## 🤖 Automation

The entire data pipeline runs automatically via GitHub Actions:

- **Schedule**: Weekly on Sunday at midnight UTC
- **Triggers**: New commits to main branch + manual runs
- **Smart Commits**: Only saves meaningful changes (new films, not just metadata updates like "how many views does this video have?")
- **Monitoring**: Full pipeline logs available in GitHub Actions

---

<div align="center">
<i>Built with ❤️ for the movie nerd community</i>
</div>
