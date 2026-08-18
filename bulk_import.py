import csv
import time
from pathlib import Path

from app.core.celery_app import celery_app
from app.services.task_manager import publish_task_update


@celery_app.task(bind=True, name="bulk_import.process_csv", track_started=True)
def process_bulk_import(self, csv_path: str) -> dict[str, int | str]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    total = len(rows)
    publish_task_update(self.request.id, {
        "task_id": self.request.id,
        "status": "PROGRESS",
        "progress": 0,
        "message": f"Loaded {total} rows",
    })
    processed = 0
    for processed, row in enumerate(rows, start=1):
        _ = row  # Replace with a database write in a production integration.
        time.sleep(0.01)
        progress = round(processed / total * 100) if total else 100
        message = f"Processing row {processed} of {total}"
        self.update_state(state="PROGRESS", meta={"progress": progress, "message": message})
        publish_task_update(self.request.id, {
            "task_id": self.request.id,
            "status": "PROGRESS",
            "progress": progress,
            "message": message,
        })

    result = {"task_id": self.request.id, "rows_processed": processed, "status": "completed"}
    publish_task_update(self.request.id, {
        "task_id": self.request.id,
        "status": "SUCCESS",
        "progress": 100,
        "message": "Bulk import completed",
        "result": result,
    })
    return result
