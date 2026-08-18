import time

from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.services.export_service import demo_rows, write_csv_report, write_pdf_report
from app.services.task_manager import publish_task_update


@celery_app.task(bind=True, name="report_generation.generate", track_started=True)
def generate_report(self, report_format: str = "csv") -> dict[str, str | int]:
    report_format = report_format.lower()
    if report_format not in {"csv", "pdf"}:
        raise ValueError("report_format must be csv or pdf")

    settings = get_settings()
    rows = demo_rows()
    publish_task_update(self.request.id, {
        "task_id": self.request.id,
        "status": "PROGRESS",
        "progress": 20,
        "message": "Preparing report data",
    })
    time.sleep(0.1)
    output_path = settings.report_dir / f"student-performance-{self.request.id}.{report_format}"
    if report_format == "csv":
        write_csv_report(output_path, rows)
    else:
        write_pdf_report(output_path, rows)

    result = {
        "task_id": self.request.id,
        "format": report_format,
        "rows": len(rows),
        "file": str(output_path),
    }
    self.update_state(state="PROGRESS", meta={"progress": 100, "message": "Report written"})
    publish_task_update(self.request.id, {
        "task_id": self.request.id,
        "status": "SUCCESS",
        "progress": 100,
        "message": "Report generation completed",
        "result": result,
    })
    return result
