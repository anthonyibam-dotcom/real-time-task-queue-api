from pathlib import Path
from uuid import uuid4

from celery.result import AsyncResult
from fastapi import APIRouter, File, HTTPException, UploadFile, status
from pydantic import BaseModel, Field

from app.core.celery_app import celery_app
from app.core.config import get_settings
from app.tasks.bulk_import import process_bulk_import
from app.tasks.report_generation import generate_report

router = APIRouter(prefix="/tasks", tags=["tasks"])
settings = get_settings()


class ReportRequest(BaseModel):
    report_format: str = Field(default="csv", pattern="^(csv|pdf)$")


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    progress: int = Field(default=0, ge=0, le=100)
    message: str | None = None
    result: dict | None = None
    error: str | None = None


def task_response(task_id: str) -> TaskStatusResponse:
    result = AsyncResult(task_id, app=celery_app)
    state = result.state
    info = result.info if isinstance(result.info, dict) else {}
    progress = int(info.get("progress", 100 if state == "SUCCESS" else 0))
    response = TaskStatusResponse(
        task_id=task_id,
        status=state,
        progress=progress,
        message=info.get("message"),
    )
    if state == "SUCCESS":
        response.result = result.result if isinstance(result.result, dict) else {"value": result.result}
    elif state == "FAILURE":
        response.error = str(result.result)
    return response


@router.post("/bulk-import", response_model=TaskStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def bulk_import(file: UploadFile = File(...)) -> TaskStatusResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")
    task_id = str(uuid4())
    destination = Path(settings.upload_dir) / f"{task_id}.csv"
    with destination.open("wb") as handle:
        while chunk := await file.read(1024 * 1024):
            handle.write(chunk)
    task = process_bulk_import.apply_async(args=[str(destination)], task_id=task_id)
    return TaskStatusResponse(task_id=task.id, status="PENDING", progress=0, message="Task accepted")


@router.post("/generate-report", response_model=TaskStatusResponse, status_code=status.HTTP_202_ACCEPTED)
async def report(request: ReportRequest) -> TaskStatusResponse:
    task = generate_report.delay(request.report_format)
    return TaskStatusResponse(task_id=task.id, status="PENDING", progress=0, message="Task accepted")


@router.get("/{task_id}", response_model=TaskStatusResponse)
async def task_status(task_id: str) -> TaskStatusResponse:
    return task_response(task_id)
