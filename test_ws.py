#!/usr/bin/env python3
"""Command-line client for the task progress WebSocket."""

import argparse
import asyncio
import json

import websockets


async def main(task_id: str, base_url: str) -> None:
    uri = f"{base_url.rstrip('/')}/ws/tasks/{task_id}"
    print(f"Connecting to {uri}")
    async with websockets.connect(uri) as socket:
        async for message in socket:
            event = json.loads(message)
            print(json.dumps(event, indent=2))
            if event.get("status") in {"SUCCESS", "FAILURE", "REVOKED"}:
                break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id")
    parser.add_argument("--base-url", default="ws://localhost:8000")
    args = parser.parse_args()
    asyncio.run(main(args.task_id, args.base_url))
