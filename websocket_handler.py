import uuid
from typing import Dict, Optional
from fastapi import WebSocket, WebSocketDisconnect, Header
import logging

from models import MessageType, ResponseType, ErrorCode, Message, WebSocketRequest
from pubsub import PubSubSystem
from utils import PubSubUtils

logger = logging.getLogger(__name__)


async def handle_websocket_connection(websocket: WebSocket, pubsub: PubSubSystem, x_api_key: Optional[str] = None):
    await websocket.accept()
    logger.info("WebSocket connection accepted")
    
    active_subscriptions: Dict[str, str] = {}
    
    try:
        while True:
            try:
                data = await websocket.receive_text()
                try:
                    request = WebSocketRequest.model_validate_json(data)
                except Exception as e:
                    await PubSubUtils.send_websocket_message(websocket, {
                        "type": ResponseType.ERROR,
                        "error": {
                            "code": ErrorCode.BAD_REQUEST,
                            "message": f"Invalid request format: {str(e)}"
                        },
                        "ts": PubSubUtils.get_timestamp()
                    })
                    continue
            except Exception as e:
                logger.error(f"Error receiving message: {e}")
                break
            
            msg_type = request.type
            request_id = request.request_id
            
            async def send_response(resp_type: str, data: dict = None):
                response = {"type": resp_type, "ts": PubSubUtils.get_timestamp()}
                if request_id:
                    response["request_id"] = request_id
                if data:
                    response.update(data)
                await PubSubUtils.send_websocket_message(websocket, response)
            
            async def send_error(error_code: ErrorCode, message: str):
                response = {
                    "type": ResponseType.ERROR,
                    "error": {
                        "code": error_code,
                        "message": message
                    },
                    "ts": PubSubUtils.get_timestamp()
                }
                if request_id:
                    response["request_id"] = request_id
                await PubSubUtils.send_websocket_message(websocket, response)
            
            if msg_type == MessageType.SUBSCRIBE:
                await _handle_subscribe(websocket, request, pubsub, active_subscriptions, send_response, send_error)
            
            elif msg_type == MessageType.UNSUBSCRIBE:
                await _handle_unsubscribe(request, pubsub, active_subscriptions, send_response, send_error)
            
            elif msg_type == MessageType.PUBLISH:
                await _handle_publish(request, pubsub, send_response, send_error)
            
            elif msg_type == MessageType.PING:
                await send_response(ResponseType.PONG)
            
            else:
                await send_error(ErrorCode.BAD_REQUEST, f"Unknown message type: {msg_type}")
    
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        for topic, client_id in list(active_subscriptions.items()):
            await pubsub.unsubscribe(client_id, topic)


async def _handle_subscribe(
    websocket: WebSocket,
    request: WebSocketRequest,
    pubsub: PubSubSystem,
    active_subscriptions: Dict[str, str],
    send_response,
    send_error
):
    topic = request.topic
    client_id = request.client_id
    last_n = request.last_n or 0
    
    subscriber = await pubsub.subscribe(websocket, client_id, topic, last_n)
    if subscriber:
        active_subscriptions[topic] = client_id
        await send_response(ResponseType.ACK, {
            "topic": topic,
            "status": "ok"
        })
    else:
        await send_error(ErrorCode.TOPIC_NOT_FOUND, f"Topic '{topic}' not found")


async def _handle_unsubscribe(
    request: WebSocketRequest,
    pubsub: PubSubSystem,
    active_subscriptions: Dict[str, str],
    send_response,
    send_error
):
    topic = request.topic
    client_id = request.client_id
    
    success = await pubsub.unsubscribe(client_id, topic)
    if success:
        active_subscriptions.pop(topic, None)
        await send_response(ResponseType.ACK, {
            "topic": topic,
            "status": "ok"
        })
    else:
        await send_error(ErrorCode.TOPIC_NOT_FOUND, f"Topic '{topic}' not found")


async def _handle_publish(request: WebSocketRequest, pubsub: PubSubSystem, send_response, send_error):
    topic = request.topic
    msg_data = request.message
    
    msg_id = msg_data.id if msg_data.id else str(uuid.uuid4())
    payload = msg_data.payload
    
    pub_message = Message(id=msg_id, payload=payload)
    success = await pubsub.publish(topic, pub_message)
    
    if success:
        await send_response(ResponseType.ACK, {
            "topic": topic,
            "status": "ok"
        })
    else:
        await send_error(ErrorCode.TOPIC_NOT_FOUND, f"Topic '{topic}' not found")
