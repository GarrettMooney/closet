# Experimental Features

This document describes the experimental features of the Criterion Closet project. These features are under active development and should be considered a work-in-progress.

## Installation

To install the experimental dependencies (for the Redis backend and FastAPI frontend), run:

```bash
uv pip install -e .[experimental]
```

## Running the Application

To run the application, follow these steps in order.

### 1. Start Redis

In a new terminal, start the Redis container using Docker Compose:

```bash
docker-compose up
```

### 2. Index the Data

To index the data in Redis, run:
```bash
uv run index
```

### 3. Start the Backend and Frontend

In two separate terminals, start the backend API and the frontend development server.

**Terminal 1: Backend API**
```bash
uv run api
```

**Terminal 2: Frontend**
```bash
cd frontend
npm install
npm run dev -- --open
```

You can then open your browser to the address provided by the Svelte development server to use the search application.
