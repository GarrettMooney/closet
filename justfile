# List all available recipes
@_:
    just --list

# Trigger the data pipeline workflow manually
[group('workflow')]
trigger-workflow:
    gh workflow run update-playlist.yml

# List recent workflow runs
[group('workflow')]
list-runs:
    gh run list --workflow=update-playlist.yml --limit=10

# Watch the latest workflow run
[group('workflow')]
watch-latest:
    gh run watch $(gh run list --workflow=update-playlist.yml --limit=1 --json databaseId --jq '.[0].databaseId')

# View the latest workflow run
[group('workflow')]
view-latest:
    gh run view $(gh run list --workflow=update-playlist.yml --limit=1 --json databaseId --jq '.[0].databaseId')

# Upgrade all packages
[group('dependencies')]
upgrade:
    uv lock --upgrade
    uv sync

# Upgrade specific package
[group('dependencies')]
upgrade-package package:
    uv lock --upgrade-package {{package}}
    uv sync
