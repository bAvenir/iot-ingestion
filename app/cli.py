"""CLI entry points for running the application."""

import uvicorn


def dev() -> None:
    """Start the development server with hot reload."""
    uvicorn.run("app.main:app", reload=True, port=8000)
