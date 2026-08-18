from fastapi.testclient import TestClient

from app.main import app


async def fake_stream(_: str):
    yield {
        "task_id": "task-ws-1",
        "status": "PROGRESS",
        "progress": 65,
        "message": "Processing row 650 of 1000",
    }
    yield {
        "task_id": "task-ws-1",
        "status": "SUCCESS",
        "progress": 100,
        "message": "Completed",
    }


def test_websocket_streams_progress_and_closes(monkeypatch):
    monkeypatch.setattr("app.api.v1.endpoints.websockets.stream_task_events", fake_stream)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/tasks/task-ws-1") as websocket:
            first = websocket.receive_json()
            second = websocket.receive_json()
    assert first["progress"] == 65
    assert second["status"] == "SUCCESS"
    assert second["progress"] == 100
