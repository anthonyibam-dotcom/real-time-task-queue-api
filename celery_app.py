from celery import Celery

from app.core.config import get_settings

settings = get_settings()
celery_app = Celery(
    "real_time_task_queue",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["app.tasks.bulk_import", "app.tasks.report_generation"],
)
celery_app.conf.update(
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    result_expires=settings.task_status_ttl_seconds,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    broker_connection_retry_on_startup=True,
)
