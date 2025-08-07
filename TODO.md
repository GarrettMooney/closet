# TODO

## Options

- [x] Try to enrich with structured generation for records that don't have structured data or (guest == "Unkown" or XXX == "Unknown")
- [x] Make structured generation resumable & idempotent
- [ ] Extract description for each movie
    - [ ] Index descriptions
- [ ] Extract year, genre, director, actors for each movie
    - [ ] Index year, genre, director, actors

## Design search experience

- toggle
    - just show movies
    - show movies and descriptions
- search params:
    - @guest:<guest>
    - @title:<title>
    - @year:<year> (NOT TRACKED)
    - @genre:<genre> (NOT TRACKED)
    - @director:<director> (NOT TRACKED)
    - @actor:<actor> (NOT TRACKED)

## UI

Make it look good lol

## Docs

- Update README.md
- Mkdocs


## Tests

- pytest for app
- ??? for frontend

## Github Actions

- Verify that they work lol
