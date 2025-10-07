# Trigger the data pipeline workflow manually
trigger-workflow:
    gh workflow run update-playlist.yml

# List recent workflow runs
list-runs:
    gh run list --workflow=update-playlist.yml --limit=10

# Watch the latest workflow run
watch-latest:
    gh run watch $(gh run list --workflow=update-playlist.yml --limit=1 --json databaseId --jq '.[0].databaseId')

# View the latest workflow run
view-latest:
    gh run view $(gh run list --workflow=update-playlist.yml --limit=1 --json databaseId --jq '.[0].databaseId')

# Upgrade all packages
upgrade:
    uv lock --upgrade
    uv sync

# Upgrade specific package
upgrade-package package:
    uv lock --upgrade-package {{package}}
    uv sync
