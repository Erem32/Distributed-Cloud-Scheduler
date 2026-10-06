"""Portable API entry point, including psycopg's Windows event-loop requirement."""

import argparse
import asyncio
import sys

import uvicorn


def create_event_loop() -> asyncio.AbstractEventLoop:
    """psycopg requires a selector loop on Windows; use platform defaults elsewhere."""
    if sys.platform == "win32":
        return asyncio.SelectorEventLoop()
    return asyncio.new_event_loop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the scheduler API.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    arguments = parser.parse_args()
    if not 1 <= arguments.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    config = uvicorn.Config(
        "distributed_cloud_scheduler.api:create_app",
        factory=True,
        host=arguments.host,
        port=arguments.port,
    )
    server = uvicorn.Server(config)
    with asyncio.Runner(loop_factory=create_event_loop) as runner:
        runner.run(server.serve())


if __name__ == "__main__":
    main()
