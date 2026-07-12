"""
SSE server that streams a numbered counter event every second.
Each event carries an incrementing id. If the client disconnects
and reconnects with a Last-Event-ID header, the server resumes
from that id instead of restarting from zero.
"""
import asyncio
import logging
from fastapi import FastAPI, Request
from sse_starlette.sse import EventSourceResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("sse-demo")

app = FastAPI()

TOTAL_EVENTS = 30  # server will count from last_id+1 to this, then stop


async def event_generator(request: Request, start_id: int):
    """
    start_id is where this stream begins counting from.
    On a fresh connection this is 0. On a reconnect, the caller
    has already read Last-Event-ID and passed start_id = that + 1.
    """
    current = start_id
    while current <= TOTAL_EVENTS:
        if await request.is_disconnected():
            log.info(f"client disconnected, stopping generator at id={current}")
            break

        yield {
            "id": str(current),
            "event": "tick",
            "data": f"count={current}",
        }
        log.info(f"sent id={current}")
        current += 1
        await asyncio.sleep(0.3)


@app.get("/stream")
async def stream(request: Request):
    last_event_id = request.headers.get("last-event-id")
    if last_event_id is not None:
        start_id = int(last_event_id) + 1
        log.info(f"RECONNECT detected: Last-Event-ID={last_event_id}, resuming from id={start_id}")
    else:
        start_id = 0
        log.info("FRESH connection: starting from id=0")

    return EventSourceResponse(event_generator(request, start_id))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")