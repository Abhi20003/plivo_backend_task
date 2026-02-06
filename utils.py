import asyncio
from datetime import datetime, timezone
from fastapi import WebSocket

from models import Subscriber


class PubSubUtils:
    @staticmethod
    def get_timestamp() -> str:
        return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

    @staticmethod
    def add_message_to_queue(queue: asyncio.Queue, message) -> None:
        try:
            queue.put_nowait(message)
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
                queue.put_nowait(message)
            except asyncio.QueueEmpty:
                pass

    @staticmethod
    async def send_websocket_message(websocket: WebSocket, message: dict) -> None:
        await websocket.send_json(message)

    @staticmethod
    def cancel_subscriber_task(subscriber: Subscriber) -> None:
        if subscriber.delivery_task and not subscriber.delivery_task.done():
            subscriber.delivery_task.cancel()

    @staticmethod
    async def cancel_and_await_subscriber_task(subscriber: Subscriber) -> None:
        if subscriber.delivery_task and not subscriber.delivery_task.done():
            subscriber.delivery_task.cancel()
            try:
                await subscriber.delivery_task
            except asyncio.CancelledError:
                pass

    @staticmethod
    async def close_subscriber_websocket(subscriber: Subscriber) -> None:
        try:
            await subscriber.websocket.close()
        except Exception:
            pass
