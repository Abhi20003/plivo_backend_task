import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, WebSocket, Header
import uvicorn

from config import MAX_QUEUE_SIZE, REPLAY_BUFFER_SIZE, BACKPRESSURE_POLICY, HOST, PORT
from pubsub import PubSubSystem
from websocket_handler import handle_websocket_connection
from rest_api import create_topic, delete_topic, list_topics, health, stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pubsub = PubSubSystem(
    max_queue_size=MAX_QUEUE_SIZE,
    replay_buffer_size=REPLAY_BUFFER_SIZE,
    backpressure_policy=BACKPRESSURE_POLICY
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Pub/Sub system starting up...")
    yield
    logger.info("Shutting down Pub/Sub system...")
    await pubsub.shutdown()


app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, x_api_key: Optional[str] = Header(None)):
    await handle_websocket_connection(websocket, pubsub, x_api_key)


@app.post("/topics")
async def create_topic_endpoint(data: dict, x_api_key: Optional[str] = Header(None)):
    return await create_topic(data, pubsub, x_api_key)


@app.delete("/topics/{name}")
async def delete_topic_endpoint(name: str, x_api_key: Optional[str] = Header(None)):
    return await delete_topic(name, pubsub, x_api_key)


@app.get("/topics")
async def list_topics_endpoint(x_api_key: Optional[str] = Header(None)):
    return await list_topics(pubsub, x_api_key)


@app.get("/health")
async def health_endpoint(x_api_key: Optional[str] = Header(None)):
    return await health(pubsub, x_api_key)


@app.get("/stats")
async def stats_endpoint(x_api_key: Optional[str] = Header(None)):
    return await stats(pubsub, x_api_key)


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)
