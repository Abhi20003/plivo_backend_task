import asyncio
import time
from collections import deque
from typing import Dict, Optional, List
from fastapi import WebSocket
import logging

from models import Message, Subscriber, ResponseType, ErrorCode
from utils import PubSubUtils

logger = logging.getLogger(__name__)


class PubSubSystem:
    def __init__(self, max_queue_size: int = 1000, replay_buffer_size: int = 100, 
                 backpressure_policy: str = "drop_oldest"):
        self.max_queue_size = max_queue_size
        self.replay_buffer_size = replay_buffer_size
        self.backpressure_policy = backpressure_policy
        self.topics: Dict[str, List[Subscriber]] = {}
        self.replay_buffers: Dict[str, deque] = {}
        self.message_counts: Dict[str, int] = {}
        self.start_time = time.time()
        self.lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()
        
    async def create_topic(self, name: str) -> bool:
        if self.shutdown_event.is_set():
            return False
        async with self.lock:
            if self.shutdown_event.is_set():
                return False
            if name in self.topics:
                return False
            self.topics[name] = []
            self.replay_buffers[name] = deque(maxlen=self.replay_buffer_size)
            self.message_counts[name] = 0

            return True
    
    async def delete_topic(self, name: str) -> bool:
        if self.shutdown_event.is_set():
            return False
        async with self.lock:
            if name not in self.topics:
                return False
            
            subscribers = list(self.topics[name])
            for subscriber in subscribers:
                PubSubUtils.cancel_subscriber_task(subscriber)
            
            await self._notify_subscribers(
                subscribers,
                {
                    "type": ResponseType.INFO,
                    "topic": name,
                    "msg": "topic_deleted",
                    "ts": PubSubUtils.get_timestamp()
                }
            )
            
            self._cleanup_topic_data(name)
            return True
    
    async def subscribe(self, websocket: WebSocket, client_id: str, topic: str, last_n: int = 0) -> Optional[Subscriber]:
        if self.shutdown_event.is_set():
            return None
        async with self.lock:
            if self.shutdown_event.is_set():
                return None
            if topic not in self.topics:
                return None
            
            existing_subscriber = None
            for sub in self.topics[topic]:
                if sub.client_id == client_id:
                    existing_subscriber = sub
                    break
            
            if existing_subscriber:
                if existing_subscriber.websocket != websocket:
                    PubSubUtils.cancel_subscriber_task(existing_subscriber)
                    existing_subscriber.websocket = websocket
                    existing_subscriber.delivery_task = asyncio.create_task(
                        self._deliver_messages_to_subscriber(existing_subscriber)
                    )
                return existing_subscriber
            
            queue = asyncio.Queue(maxsize=self.max_queue_size)
            subscriber = Subscriber(websocket, client_id, queue, topic)
            self.topics[topic].append(subscriber)
            
            if last_n > 0 and topic in self.replay_buffers:
                replay_buffer = self.replay_buffers[topic]
                replay_count = min(last_n, len(replay_buffer))
                for i in range(-replay_count, 0):
                    if i < 0:
                        msg = replay_buffer[i]
                        PubSubUtils.add_message_to_queue(queue, msg)
            
            subscriber.delivery_task = asyncio.create_task(
                self._deliver_messages_to_subscriber(subscriber)
            )
            
            return subscriber
    
    async def unsubscribe(self, client_id: str, topic: str) -> bool:
        if self.shutdown_event.is_set():
            return False
        async with self.lock:
            if topic not in self.topics:
                return False
            
            subscribers_to_remove = [s for s in self.topics[topic] if s.client_id == client_id]
            for subscriber in subscribers_to_remove:
                self.topics[topic].remove(subscriber)
                PubSubUtils.cancel_subscriber_task(subscriber)
            
            return True
    
    async def publish(self, topic: str, message: Message) -> bool:
        if self.shutdown_event.is_set():
            return False
        slow_consumers = []
        async with self.lock:
            if self.shutdown_event.is_set():
                return False
            if topic not in self.topics:
                return False
            
            if topic in self.replay_buffers:
                self.replay_buffers[topic].append(message)
            
            self.message_counts[topic] = self.message_counts.get(topic, 0) + 1
            
            subscribers = list(self.topics[topic])

            for subscriber in subscribers:
                try:
                    subscriber.queue.put_nowait(message)
                except asyncio.QueueFull:
                    if self.backpressure_policy == "disconnect_slow_consumer":
                        slow_consumers.append(subscriber)
                    else:
                        try:
                            subscriber.queue.get_nowait()
                            subscriber.queue.put_nowait(message)
                        except asyncio.QueueEmpty:
                            pass
                except Exception as e:
                    logger.warning(f"Failed to queue message for subscriber {subscriber.client_id}: {e}")

        slow_consumers_to_disconnect = slow_consumers.copy() if slow_consumers else []
        if slow_consumers_to_disconnect:
            for subscriber in slow_consumers_to_disconnect:
                await self._disconnect_slow_consumer(subscriber, topic)

        return True
    
    async def get_topics_info(self) -> list:
        async with self.lock:
            return [
                {"name": name, "subscribers": len(subscribers)}
                for name, subscribers in self.topics.items()
            ]
    
    async def get_stats(self) -> dict:
        async with self.lock:
            return {
                "topics": {
                    name: {
                        "messages": self.message_counts.get(name, 0),
                        "subscribers": len(subscribers)
                    }
                    for name, subscribers in self.topics.items()
                }
            }
    
    async def get_health(self) -> dict:
        async with self.lock:
            total_subscribers = sum(len(subs) for subs in self.topics.values())
            return {
                "uptime_sec": int(time.time() - self.start_time),
                "topics": len(self.topics),
                "subscribers": total_subscribers
            }
    
    def _cleanup_topic_data(self, topic_name: str):
        if topic_name in self.topics:
            del self.topics[topic_name]
        if topic_name in self.replay_buffers:
            del self.replay_buffers[topic_name]
        if topic_name in self.message_counts:
            del self.message_counts[topic_name]
    
    async def _notify_subscribers(self, subscribers: list, message: dict):
        for subscriber in subscribers:
            try:
                await PubSubUtils.send_websocket_message(subscriber.websocket, message)
            except Exception as e:
                logger.warning(f"Failed to notify subscriber {subscriber.client_id}: {e}")
    
    async def _deliver_messages_to_subscriber(self, subscriber: Subscriber):
        try:
            while True:
                try:
                    message = await asyncio.wait_for(subscriber.queue.get(), timeout=1.0)
                    
                    await PubSubUtils.send_websocket_message(subscriber.websocket, {
                        "type": ResponseType.EVENT,
                        "topic": subscriber.topic,
                        "message": {
                            "id": message.id,
                            "payload": message.payload
                        },
                        "ts": PubSubUtils.get_timestamp()
                    })
                    subscriber.queue.task_done()
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.warning(f"Error delivering message to {subscriber.client_id}: {e}")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Delivery task error for {subscriber.client_id}: {e}")
    
    async def _disconnect_slow_consumer(self, subscriber: Subscriber, topic: str):
        try:
            await PubSubUtils.send_websocket_message(subscriber.websocket, {
                "type": ResponseType.ERROR,
                "error": {
                    "code": ErrorCode.SLOW_CONSUMER,
                    "message": f"Subscriber queue overflow for topic '{topic}'. Disconnecting."
                },
                "ts": PubSubUtils.get_timestamp()
            })
        except Exception as e:
            logger.warning(f"Failed to send SLOW_CONSUMER error to {subscriber.client_id}: {e}")
        
        await self.unsubscribe(subscriber.client_id, topic)
        
        try:
            await PubSubUtils.close_subscriber_websocket(subscriber)
        except Exception:
            pass

    async def shutdown(self):
        self.shutdown_event.set()
        
        async with self.lock:
            for topic, subscribers in self.topics.items():
                for subscriber in list(subscribers):
                    await self._flush_subscriber_queue(subscriber)
                    await PubSubUtils.cancel_and_await_subscriber_task(subscriber)
                    await PubSubUtils.close_subscriber_websocket(subscriber)
    
    async def _flush_subscriber_queue(self, subscriber: Subscriber):
        try:
            while not subscriber.queue.empty():
                try:
                    message = subscriber.queue.get_nowait()
                    await PubSubUtils.send_websocket_message(subscriber.websocket, {
                        "type": ResponseType.EVENT,
                        "topic": subscriber.topic,
                        "message": {
                            "id": message.id,
                            "payload": message.payload
                        },
                        "ts": PubSubUtils.get_timestamp()
                    })
                    subscriber.queue.task_done()
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    logger.warning(f"Error flushing message to {subscriber.client_id}: {e}")
                    break
        except Exception as e:
            logger.warning(f"Error flushing queue for {subscriber.client_id}: {e}")