import redis
import srsly
from redis.commands.search.field import TagField, TextField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from closet.config import ENRICHED_PLAYLIST_JSON_PATH, INDEX_NAME
from closet.logging import console


def create_redis_index(r: redis.Redis):
    """Creates the Redis Search index if it does not already exist.

    Parameters
    ----------
    r : redis.Redis
        An active Redis client instance.
    """
    schema = (
        TextField("$.title", as_name="title"),
        TextField("$.subtitles", as_name="subtitles"),
        TagField("$.guest", as_name="guest"),
        TagField("$.year", as_name="year"),
        TextField("$.movies[*].title", as_name="movies"),
    )
    try:
        r.ft(INDEX_NAME).info()
        console.log(f"Index '{INDEX_NAME}' already exists.")
    except redis.exceptions.ResponseError:
        r.ft(INDEX_NAME).create_index(
            schema,
            definition=IndexDefinition(
                prefix=[f"{INDEX_NAME}:"], index_type=IndexType.JSON
            ),
        )
        console.log(f"Index '{INDEX_NAME}' created successfully.")


def index_data(r: redis.Redis):
    """Indexes the enriched playlist data.

    Parameters
    ----------
    r : redis.Redis
        An active Redis client instance.
    """
    if not ENRICHED_PLAYLIST_JSON_PATH.exists():
        console.log("No enriched playlist data found to index.", style="red")
        return

    playlist_data = list(srsly.read_jsonl(ENRICHED_PLAYLIST_JSON_PATH))
    pipeline = r.pipeline()
    for i, record in enumerate(playlist_data):
        pipeline.json().set(f"{INDEX_NAME}:{i}", "$", record)
    pipeline.execute()

    console.log(f"Indexed {len(playlist_data)} records.")


def main():
    """Main entry point for the script."""
    r = redis.Redis(decode_responses=True)
    create_redis_index(r)
    index_data(r)


if __name__ == "__main__":
    main()
