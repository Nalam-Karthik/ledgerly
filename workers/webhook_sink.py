"""Demo webhook receiver for docker-compose: logs every delivery it gets.
Set SINK_FAIL_RATE (0..1) to simulate a flaky endpoint and watch retries / DLQ."""

import logging
import os
import random

from fastapi import FastAPI, Request, Response

log = logging.getLogger("uvicorn.error")
FAIL_RATE = float(os.getenv("SINK_FAIL_RATE", "0"))

app = FastAPI()


@app.post("/hook")
async def hook(request: Request):
    body = await request.body()
    if random.random() < FAIL_RATE:
        log.info("sink: simulated failure for %s", request.headers.get("ledgerly-event-id"))
        return Response(status_code=503)
    log.info("sink: %s sig=%s body=%s", request.headers.get("ledgerly-event-id"),
             request.headers.get("ledgerly-signature"), body.decode())
    return {"received": True}
