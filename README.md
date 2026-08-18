# Real-Time Task Queue API

The Real-Time Task Queue API is an asynchronous background job processing engine designed to handle compute-heavy operations without blocking HTTP requests. Powered by FastAPI, Celery, and Redis, the system offloads operations like bulk CSV imports and report generation while broadcasting live completion progress to clients using bidirectional WebSockets.
