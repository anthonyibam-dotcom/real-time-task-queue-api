from fastapi import APIRouter

from app.api.v1.endpoints import tasks, websockets

api_router = APIRouter()
api_router.include_router(tasks.router, prefix="/api/v1")
api_router.include_router(websockets.router)
