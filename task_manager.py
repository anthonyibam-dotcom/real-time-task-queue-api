import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as redis

from app.core.config import get_settings


TERMINAL_STATES = {"SUCCESS", "FAILURE", "REVOKED"}


def task_channel(task_id: str) -> str:
    return f"task-events:{task_id}"


def task_status_key(task_id: str) -> str:
    return f"task-status:{task_id}"


def publish_task_update(task_id: str, payload: dict[str, Any]) -> None:
    """Persist and publish an update from a synchronous Celery worker."""
    settings = get_settings()
    client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        message = json.dumps(payload, default=str)
        client.set(task_status_key(task_id), message, ex=settings.task_status_ttl_seconds)
        client.publish(task_channel(task_id), message)
    finally:
        client.close()


async def get_task_snapshot(task_id: str) -> dict[str, Any] | None:
    settings = get_settings()
    client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        value = await client.get(task_status_key(task_id))
        return json.loads(value) if value else None
    finally:
        await client.aclose()


async def stream_task_events(task_id: str, timeout_seconds: int = 30) -> AsyncIterator[dict[str, Any]]:
    """Yield Redis Pub/Sub events until a terminal state or idle timeout."""
    settings = get_settings()
    client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    pubsub = client.pubsub()
    await pubsub.subscribe(task_channel(task_id))
    try:
        snapshot = await get_task_snapshot(task_id)
        if snapshot:
            yield snapshot
            if snapshot.get("status") in TERMINAL_STATES:
                return
        idle = 0
        while idle < timeout_seconds:
            event = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if event and event.get("data"):
                idle = 0
                payload = json.loads(event["data"])
                yield payload
                if payload.get("status") in TERMINAL_STATES:
                    return
            else:
                idle += 1
                await asyncio.sleep(0.05)
    finally:
        await pubsub.unsubscribe(task_channel(task_id))
        await pubsub.close()
        await client.aclose()
