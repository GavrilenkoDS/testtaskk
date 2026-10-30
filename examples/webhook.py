from fastapi import FastAPI, Response

app = FastAPI()
events: dict[str, dict] = {}


@app.post("/webhook")
async def webhook(body: dict):
    events.setdefault(body["event_id"], body)
    return {"received": True}


@app.post("/fail")
async def fail():
    return Response(status_code=503)


@app.get("/events")
async def received():
    return list(events.values())
