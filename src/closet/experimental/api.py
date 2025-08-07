import redis
from fastapi import FastAPI
from redis.commands.search.query import Query

from closet.config import INDEX_NAME
from closet.logging import console

app = FastAPI()


@app.get("/search")
def search(q: str):
    """
    Search the Redis index.
    """
    r = redis.Redis(decode_responses=True)

    # Check if the index exists
    try:
        r.ft(INDEX_NAME).info()
    except redis.exceptions.ResponseError:
        return {"error": f"Index '{INDEX_NAME}' not found. Please run `uv run index`."}

    # Basic query parsing
    if q.startswith("movie:"):
        field = "movies"
        query_str = q.split(":", 1)[1].strip()
    elif q.startswith("guest:"):
        field = "guest"
        query_str = q.split(":", 1)[1].strip()
    else:
        field = "subtitles"
        query_str = q.strip()

    # Create the query
    query = Query(f"@{field}:{query_str}")

    # Execute the query
    try:
        result = r.ft(INDEX_NAME).search(query)
        return {"results": [doc.__dict__ for doc in result.docs]}
    except Exception as e:
        console.log(f"Error searching Redis: {e}", style="red")
        return {"error": str(e)}


def main():
    """
    Run the FastAPI application.
    """
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
