# Upgrade all packages
upgrade:
    uv lock --upgrade
    uv sync

# Upgrade specific package
upgrade-package package:
    uv lock --upgrade-package {{package}}
    uv sync
