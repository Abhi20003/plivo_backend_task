# In-Memory Pub/Sub System

A simplified in-memory Pub/Sub system implemented in Python with WebSocket and HTTP REST APIs.

## Features

- **WebSocket-based Pub/Sub**: Real-time message publishing and subscription
- **HTTP REST APIs**: Topic management, health checks, and statistics
- **In-Memory Storage**: All data stored in memory (no external dependencies)
- **Thread-Safe**: Concurrent operations supported
- **Message Replay**: Support for replaying last N messages on subscription
- **Backpressure Handling**: Bounded queues with oldest message drop policy
- **Graceful Shutdown**: Proper cleanup of connections and resources

## How to Run

### Using Docker

```bash
# Build the image
docker build -t pubsub-system .

# Run the container
docker run -p 8000:8000 pubsub-system
```

### Using Python directly

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python main.py
```

The server will start on `http://0.0.0.0:8000`

## API Usage

### WebSocket Endpoint: `/ws`

Connect to the WebSocket endpoint to publish, subscribe, unsubscribe, and ping.

#### Client → Server Message Format

```json
{
  "type": "subscribe" | "unsubscribe" | "publish" | "ping",
  "topic": "orders",
  "message": {
    "id": "uuid",
    "payload": {}
  },
  "client_id": "s1",
  "last_n": 0,
  "request_id": "uuid-optional"
}
```

#### Message Types

**Subscribe**
```json
{
  "type": "subscribe",
  "topic": "orders",
  "client_id": "client1",
  "last_n": 10
}
```
- `topic` (required): Topic name to subscribe to
- `client_id` (required): Unique client identifier
- `last_n` (optional): Number of last messages to replay (default: 0)

**Unsubscribe**
```json
{
  "type": "unsubscribe",
  "topic": "orders",
  "client_id": "client1"
}
```
- `topic` (required): Topic name to unsubscribe from
- `client_id` (required): Client identifier

**Publish**
```json
{
  "type": "publish",
  "topic": "orders",
  "message": {
    "id": "msg-123",
    "payload": {
      "order_id": "12345",
      "status": "pending"
    }
  }
}
```
- `topic` (required): Topic name to publish to
- `message` (required): Message object with `id` and `payload`

**Ping**
```json
{
  "type": "ping",
  "request_id": "ping-123"
}
```

#### Server → Client Message Format

**ACK (Acknowledgement)**
```json
{
  "type": "ack",
  "topic": "orders",
  "request_id": "uuid-optional"
}
```

**EVENT (Message Delivery)**
```json
{
  "type": "event",
  "topic": "orders",
  "message": {
    "id": "msg-123",
    "payload": {
      "order_id": "12345",
      "status": "pending"
    }
  }
}
```

**ERROR**
```json
{
  "type": "error",
  "error_code": "BAD_REQUEST" | "TOPIC_NOT_FOUND" | "SLOW_CONSUMER" | "UNAUTHORIZED" | "INTERNAL",
  "message": "Error description",
  "request_id": "uuid-optional"
}
```

**PONG**
```json
{
  "type": "pong",
  "request_id": "uuid-optional"
}
```

**INFO (Server-initiated)**
```json
{
  "type": "info",
  "info_type": "topic_deleted",
  "topic": "orders"
}
```

### HTTP REST APIs

#### Create Topic
```bash
POST /topics
Content-Type: application/json

{
  "name": "orders"
}
```

**Responses:**
- `201 Created`: Topic created successfully
- `409 Conflict`: Topic already exists

#### Delete Topic
```bash
DELETE /topics/{name}
```

**Responses:**
- `200 OK`: Topic deleted successfully
- `404 Not Found`: Topic does not exist

**Note:** All subscribers to the deleted topic will be automatically unsubscribed and receive an `info` message indicating topic deletion.

#### List Topics
```bash
GET /topics
```

**Response:**
```json
{
  "topics": [
    {
      "name": "orders",
      "subscribers": 3
    }
  ]
}
```

#### Health Check
```bash
GET /health
```

**Response:**
```json
{
  "uptime_sec": 123,
  "topics": 2,
  "subscribers": 4
}
```

#### Statistics
```bash
GET /stats
```

**Response:**
```json
{
  "topics": {
    "orders": {
      "messages": 42,
      "subscribers": 3
    }
  }
}
```

## WebSocket Protocol

### Connection Flow

1. **Connect** to `ws://localhost:8000/ws`
2. **Create Topic** (via HTTP REST API) before subscribing
3. **Subscribe** to topics using the `subscribe` message type
4. **Receive Events** as `event` messages when messages are published
5. **Publish** messages using the `publish` message type
6. **Unsubscribe** when no longer interested in a topic

### Example WebSocket Session

```python
import asyncio
import websockets
import json

async def client_example():
    uri = "ws://localhost:8000/ws"
    async with websockets.connect(uri) as websocket:
        # Subscribe
        await websocket.send(json.dumps({
            "type": "subscribe",
            "topic": "orders",
            "client_id": "client1",
            "last_n": 5
        }))
        
        # Receive ACK
        response = await websocket.recv()
        print(f"Received: {response}")
        
        # Publish
        await websocket.send(json.dumps({
            "type": "publish",
            "topic": "orders",
            "message": {
                "id": "msg-1",
                "payload": {"order": "123"}
            }
        }))
        
        # Receive event
        event = await websocket.recv()
        print(f"Event: {event}")

asyncio.run(client_example())
```

## Design Choices

### Backpressure Policy

**Chosen Policy: Drop Oldest Message**

When a subscriber's queue is full (default max size: 1000 messages), the system will:
1. Remove the oldest message from the queue
2. Add the new message to the queue

This ensures that subscribers always receive the most recent messages, even if they cannot keep up with the message rate. The alternative (disconnecting with `SLOW_CONSUMER` error) was not chosen as it would be too disruptive for temporary slowdowns.

### Concurrency Safety

- All topic and subscriber operations are protected by `asyncio.Lock`
- Each subscriber has its own message queue (`asyncio.Queue`)
- Message delivery uses separate async tasks per subscriber to avoid blocking

### Message Replay

- Each topic maintains a ring buffer of the last 100 messages (configurable)
- When subscribing with `last_n > 0`, the last N messages are replayed
- Replay messages are delivered immediately upon subscription, before new messages

### Topic Isolation

- Each topic maintains its own set of subscribers
- Messages published to one topic are only delivered to subscribers of that topic
- Deleting a topic automatically unsubscribes all its subscribers

### Graceful Shutdown

On shutdown:
1. Stop accepting new operations (via shutdown event)
2. Cancel all message delivery tasks
3. Close all WebSocket connections
4. Clean up all resources

## Code Documentation

### File Structure

#### `main.py`
Application entry point that sets up FastAPI server with WebSocket and REST endpoints.

**Functions:**
- `lifespan(app: FastAPI)`: Async context manager handling application startup and shutdown. Triggers graceful shutdown of PubSubSystem on app termination.
- `websocket_endpoint(websocket, x_api_key)`: WebSocket endpoint handler accepting optional X-API-Key header for authentication.
- `create_topic_endpoint`, `delete_topic_endpoint`, `list_topics_endpoint`, `health_endpoint`, `stats_endpoint`: REST API endpoints delegating to respective handlers.

**Approach:** Uses FastAPI's lifespan context manager to ensure proper cleanup on shutdown. All endpoints accept optional X-API-Key header for future authentication.

#### `pubsub.py`
Core Pub/Sub system implementation handling all topic and subscriber operations.

**Class: `PubSubSystem`**

**Initialization:**
- `__init__(max_queue_size, replay_buffer_size, backpressure_policy)`: Initializes in-memory data structures with configurable queue sizes and backpressure policy. Creates asyncio.Lock for concurrency safety and shutdown_event for graceful shutdown coordination.

**Topic Management:**
- `create_topic(name)`: Creates a new topic with empty subscriber list, replay buffer (deque with maxlen for ring buffer), and message counter. Protected by lock to prevent race conditions.
- `delete_topic(name)`: Deletes topic and notifies all subscribers with INFO message before cleanup. Cancels all delivery tasks and removes topic data structures atomically.

**Subscription Management:**
- `subscribe(websocket, client_id, topic, last_n)`: Subscribes client to topic. Creates per-subscriber asyncio.Queue (bounded by max_queue_size) and spawns dedicated delivery task. Handles reconnection by updating websocket and restarting delivery task. Replays last N messages from ring buffer if requested. **Fan-out approach:** Each subscriber gets independent queue and delivery task.
- `unsubscribe(client_id, topic)`: Removes subscriber from topic's subscriber list and cancels delivery task. **Isolation approach:** Only affects subscribers of the specified topic.

**Publishing:**
- `publish(topic, message)`: Publishes message to all subscribers of a topic. Adds message to replay buffer (ring buffer). For each subscriber, attempts to enqueue message. **Backpressure approach:** On QueueFull, either drops oldest message (default) or marks subscriber for disconnection based on policy. Handles slow consumer disconnection outside lock to prevent deadlock. **Fan-out approach:** Iterates through all subscribers of the topic, ensuring each receives the message once.

**Message Delivery:**
- `_deliver_messages_to_subscriber(subscriber)`: Background task per subscriber that continuously dequeues messages and sends via WebSocket. Uses timeout to allow periodic cancellation checks. **Concurrency approach:** Separate async task per subscriber prevents blocking and enables independent processing speeds.

**Backpressure Handling:**
- `_disconnect_slow_consumer(subscriber, topic)`: Sends SLOW_CONSUMER error message and unsubscribes slow consumer. Used when backpressure_policy is "disconnect_slow_consumer".

**Statistics & Health:**
- `get_topics_info()`: Returns list of topics with subscriber counts. Lock-protected for consistency.
- `get_stats()`: Returns per-topic message counts and subscriber counts.
- `get_health()`: Returns uptime, total topics, and total subscribers.

**Shutdown:**
- `shutdown()`: Sets shutdown event to reject new operations. Flushes all subscriber queues (best-effort), cancels all delivery tasks, and closes all WebSocket connections. **Graceful shutdown approach:** Stops accepting new ops first, then best-effort flush, then cleanup.

**Helper Methods:**
- `_cleanup_topic_data(topic_name)`: Removes all topic-related data structures.
- `_notify_subscribers(subscribers, message)`: Sends message to multiple subscribers with error handling.
- `_flush_subscriber_queue(subscriber)`: Best-effort delivery of remaining queued messages during shutdown.

**Concurrency Safety Approach:** All state-modifying operations are protected by `asyncio.Lock`. Double-check pattern with shutdown_event prevents operations during shutdown. Per-subscriber queues and tasks ensure isolation.

**Isolation Approach:** Each topic maintains separate subscriber list and replay buffer. Publishing iterates only through subscribers of the target topic. No cross-topic data access.

#### `websocket_handler.py`
WebSocket message handling and protocol implementation.

**Functions:**
- `handle_websocket_connection(websocket, pubsub, x_api_key)`: Main WebSocket handler accepting connections and processing messages. Maintains active_subscriptions dict to track per-connection subscriptions. Parses JSON messages using Pydantic validation. Routes messages to appropriate handlers. On disconnect, automatically unsubscribes from all topics. **Approach:** Single connection handler maintains subscription state and routes to specialized handlers.

- `_handle_subscribe(websocket, request, pubsub, active_subscriptions, send_response, send_error)`: Processes subscribe requests. Validates topic existence, calls pubsub.subscribe with last_n support, tracks subscription in active_subscriptions, sends ACK on success.

- `_handle_unsubscribe(request, pubsub, active_subscriptions, send_response, send_error)`: Processes unsubscribe requests. Removes from active_subscriptions and sends ACK.

- `_handle_publish(request, pubsub, send_response, send_error)`: Processes publish requests. Generates message ID if not provided, creates Message object, calls pubsub.publish, sends ACK on success.

**Approach:** Request validation via Pydantic ensures type safety. Helper functions for response/error sending maintain consistent message format. Active subscription tracking enables automatic cleanup on disconnect.

#### `rest_api.py`
HTTP REST API handlers for topic management and system information.

**Functions:**
- `create_topic(data, pubsub, x_api_key)`: Validates topic name, calls pubsub.create_topic, returns 201 on success or 409 on conflict.
- `delete_topic(name, pubsub, x_api_key)`: Calls pubsub.delete_topic, returns 200 on success or 404 if not found.
- `list_topics(pubsub, x_api_key)`: Returns all topics with subscriber counts.
- `health(pubsub, x_api_key)`: Returns system health metrics (uptime, topics, subscribers).
- `stats(pubsub, x_api_key)`: Returns per-topic statistics (message counts, subscriber counts).

**Approach:** Thin wrapper layer around PubSubSystem methods. All functions accept optional x_api_key for future authentication. Consistent error handling via HTTPException.

#### `models.py`
Data models and enums for type safety and validation.

**Enums:**
- `MessageType`: Client-to-server message types (subscribe, unsubscribe, publish, ping).
- `ResponseType`: Server-to-client message types (ack, event, error, pong, info).
- `ErrorCode`: Error codes (BAD_REQUEST, TOPIC_NOT_FOUND, SLOW_CONSUMER, UNAUTHORIZED, INTERNAL).

**Classes:**
- `Message`: Dataclass representing published message with id, payload, and timestamp.
- `Subscriber`: Dataclass representing subscriber with websocket, client_id, queue, topic, and delivery_task reference.
- `MessagePayload`: Pydantic model for message payload in WebSocket requests.
- `WebSocketRequest`: Pydantic model with validation ensuring required fields based on message type.

**Approach:** Pydantic validation ensures request format correctness before processing. Dataclasses provide type hints and immutable message representation.

#### `utils.py`
Utility functions for common operations.

**Class: `PubSubUtils`**

**Static Methods:**
- `get_timestamp()`: Returns ISO 8601 formatted UTC timestamp.
- `add_message_to_queue(queue, message)`: Adds message to queue with drop-oldest fallback on QueueFull.
- `send_websocket_message(websocket, message)`: Sends JSON message via WebSocket.
- `cancel_subscriber_task(subscriber)`: Cancels subscriber's delivery task if running.
- `cancel_and_await_subscriber_task(subscriber)`: Cancels and waits for task completion (used during shutdown).
- `close_subscriber_websocket(subscriber)`: Closes WebSocket connection with error handling.

**Approach:** Utility class centralizes common operations for consistency and reusability. Error handling in utility methods prevents exceptions from propagating.

#### `config.py`
Configuration constants for system parameters.

**Constants:**
- `MAX_QUEUE_SIZE`: Maximum messages per subscriber queue (default: 1000).
- `REPLAY_BUFFER_SIZE`: Maximum messages in replay buffer per topic (default: 100).
- `BACKPRESSURE_POLICY`: Policy for queue overflow ("drop_oldest" or "disconnect_slow_consumer").
- `HOST`, `PORT`: Server binding configuration.

**Approach:** Centralized configuration enables easy tuning of system parameters without code changes.

## Assumptions

1. **No Authentication Required**: The `X-API-Key` header is accepted but not validated. Authentication can be added by implementing validation logic.

2. **In-Memory Only**: All data is lost on restart. No persistence is implemented.

3. **Single Process**: The system runs in a single process. For horizontal scaling, an external message broker would be needed.

4. **Message Ordering**: Messages are delivered in the order they are published, per subscriber queue. However, due to async delivery, subscribers may receive messages in slightly different orders if they process at different speeds.

5. **Queue Size**: Default maximum queue size per subscriber is 1000 messages. This can be adjusted in the `PubSubSystem` constructor.

6. **Replay Buffer Size**: Default replay buffer size per topic is 100 messages. This can be adjusted in the `PubSubSystem` constructor.

## Error Codes

- `BAD_REQUEST`: Invalid request format or missing required fields
- `TOPIC_NOT_FOUND`: Attempted operation on a non-existent topic
- `SLOW_CONSUMER`: (Reserved for future use) Consumer cannot keep up
- `UNAUTHORIZED`: (Reserved for future use) Authentication required
- `INTERNAL`: Internal server error

## Testing

Example test using `curl` and `websocat`:

```bash
# Create a topic
curl -X POST http://localhost:8000/topics -H "Content-Type: application/json" -d '{"name":"test"}'

# Check health
curl http://localhost:8000/health

# Connect via WebSocket (using websocat)
echo '{"type":"subscribe","topic":"test","client_id":"client1"}' | websocat ws://localhost:8000/ws
```

## Limitations

- No persistence: All data is lost on restart
- Single instance: Cannot scale horizontally without external broker
- No authentication: X-API-Key header is accepted but not validated
- Memory bound: System memory limits the number of topics and messages
