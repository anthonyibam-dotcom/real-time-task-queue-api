from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_report_task_creation_returns_accepted_task():
    fake_task = MagicMock(id="task-report-123")
    with patch("app.api.v1.endpoints.tasks.generate_report.delay", return_value=fake_task):
        response = client.post("/api/v1/tasks/generate-report", json={"report_format": "csv"})
    assert response.status_code == 202
    assert response.json()["task_id"] == "task-report-123"
    assert response.json()["status"] == "PENDING"


def test_bulk_import_rejects_non_csv():
    response = client.post(
        "/api/v1/tasks/bulk-import",
        files={"file": ("grades.txt", b"not,csv", "text/plain")},
    )
    assert response.status_code == 400
    assert "CSV" in response.json()["detail"]


def test_task_status_reads_celery_result():
    fake_result = MagicMock(state="PROGRESS", info={"progress": 65, "message": "Processing row 650 of 1000"})
    with patch("app.api.v1.endpoints.tasks.AsyncResult", return_value=fake_result):
        response = client.get("/api/v1/tasks/task-abc")
    assert response.status_code == 200
    assert response.json()["status"] == "PROGRESS"
    assert response.json()["progress"] == 65
    assert response.json()["message"] == "Processing row 650 of 1000"
