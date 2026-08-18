from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.task_manager import TERMINAL_STATES, stream_task_events

router = APIRouter(tags=["websockets"])


@router.websocket("/ws/tasks/{task_id}")
async def task_progress(websocket: WebSocket, task_id: str) -> None:
    await websocket.accept()
    try:
        terminal = False
        async for event in stream_task_events(task_id):
            await websocket.send_json(event)
            if event.get("status") in TERMINAL_STATES:
                terminal = True
                break
        if not terminal:
            await websocket.send_json({
                "task_id": task_id,
                "status": "PENDING",
                "progress": 0,
                "message": "No event received before timeout",
            })
    except WebSocketDisconnect:
        return
    finally:
        await websocket.close()
