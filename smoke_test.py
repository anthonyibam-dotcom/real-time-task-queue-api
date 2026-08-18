import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.main import app  # noqa: E402


with TestClient(app) as client:
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/").status_code == 200
    print("smoke: ok")
