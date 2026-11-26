import json
import logging
import sys
import time
import uuid
from contextvars import ContextVar

from prometheus_client import Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

request_id_ctx: ContextVar[str | None] = ContextVar("request_id", default=None)

HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
TRANSFERS = Counter("ledger_transactions_total", "Ledger transactions by kind and outcome", ["kind", "outcome"])
IDEMPOTENT_REPLAYS = Counter("idempotent_replays_total", "Requests answered from the idempotency store")
RATE_LIMITED = Counter("rate_limited_total", "Requests rejected by the rate limiter")
OUTBOX_PUBLISHED = Counter("outbox_events_published_total", "Outbox events published to Kafka")
OUTBOX_LAG = Gauge("outbox_unpublished_events", "Outbox events awaiting publication")
WEBHOOK_DELIVERIES = Counter("webhook_deliveries_total", "Webhook delivery attempts", ["outcome"])


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": request_id_ctx.get(),
        }
        payload.update(getattr(record, "extra_fields", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Assigns a request id, records RED metrics and emits one access log line per request."""

    async def dispatch(self, request: Request, call_next) -> Response:
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex
        token = request_id_ctx.set(rid)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["x-request-id"] = rid
            return response
        finally:
            elapsed = time.perf_counter() - start
            # Use the route template, not the raw path, to keep label cardinality bounded.
            route = request.scope.get("route")
            route_label = getattr(route, "path", "unmatched")
            HTTP_REQUESTS.labels(request.method, route_label, str(status)).inc()
            HTTP_LATENCY.labels(request.method, route_label).observe(elapsed)
            logging.getLogger("access").info(
                "request",
                extra={
                    "extra_fields": {
                        "method": request.method,
                        "route": route_label,
                        "status": status,
                        "duration_ms": round(elapsed * 1000, 2),
                    }
                },
            )
            request_id_ctx.reset(token)
