# Criterion Closet

This project contains tools for interacting with the Criterion Closet YouTube playlist.

## Installation

Install the project and its dependencies using `uv`:

```bash
uv pip install -e .
```

## Automated Updates

This project uses a GitHub Actions workflow to automatically update the `data/playlist.json` file every day. The workflow is defined in `.github/workflows/update-playlist.yml`.

When changes are detected, the workflow will automatically commit the updated file to the `main` branch.
