import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import asyncio
from fastapi import WebSocket
from pydantic import BaseModel, Field, ConfigDict, model_validator


class MessageType(str, Enum):
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    PUBLISH = "publish"
    PING = "ping"


class ResponseType(str, Enum):
    ACK = "ack"
    EVENT = "event"
    ERROR = "error"
    PONG = "pong"
    INFO = "info"


class ErrorCode(str, Enum):
    BAD_REQUEST = "BAD_REQUEST"
    TOPIC_NOT_FOUND = "TOPIC_NOT_FOUND"
    SLOW_CONSUMER = "SLOW_CONSUMER"
    UNAUTHORIZED = "UNAUTHORIZED"
    INTERNAL = "INTERNAL"


@dataclass
class Message:
    id: str
    payload: dict
    timestamp: float = field(default_factory=time.time)


@dataclass
class Subscriber:
    websocket: WebSocket
    client_id: str
    queue: asyncio.Queue
    topic: str
    delivery_task: Optional[asyncio.Task] = None


class MessagePayload(BaseModel):
    id: Optional[str] = None
    payload: dict = Field(default_factory=dict)


class WebSocketRequest(BaseModel):
    model_config = ConfigDict(exclude_none=True)
    
    type: MessageType
    topic: Optional[str] = None
    message: Optional[MessagePayload] = None
    client_id: Optional[str] = None
    last_n: Optional[int] = 0
    request_id: Optional[str] = None
    
    @model_validator(mode='after')
    def validate_required_fields(self):
        msg_type = self.type
        
        if msg_type in [MessageType.SUBSCRIBE, MessageType.UNSUBSCRIBE]:
            if not self.topic:
                raise ValueError(f"topic is required for {msg_type.value}")
            if not self.client_id:
                raise ValueError(f"client_id is required for {msg_type.value}")
        
        elif msg_type == MessageType.PUBLISH:
            if not self.topic:
                raise ValueError("topic is required for publish")
            if not self.message:
                raise ValueError("message is required for publish")
                
        return self
