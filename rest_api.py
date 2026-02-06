from typing import Optional
from fastapi import HTTPException, Header
from fastapi.responses import JSONResponse

from pubsub import PubSubSystem


async def create_topic(data: dict, pubsub: PubSubSystem, x_api_key: Optional[str] = None):
    name = data.get("name")
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    
    created = await pubsub.create_topic(name)
    if created:
        return JSONResponse(status_code=201, content={"message": "Topic created", "name": name})
    else:
        raise HTTPException(status_code=409, detail="Topic already exists")


async def delete_topic(name: str, pubsub: PubSubSystem, x_api_key: Optional[str] = None):
    deleted = await pubsub.delete_topic(name)
    if deleted:
        return {"message": "Topic deleted", "name": name}
    else:
        raise HTTPException(status_code=404, detail="Topic not found")


async def list_topics(pubsub: PubSubSystem, x_api_key: Optional[str] = None):
    topics = await pubsub.get_topics_info()
    return {"topics": topics}


async def health(pubsub: PubSubSystem, x_api_key: Optional[str] = None):
    health_data = await pubsub.get_health()
    return health_data


async def stats(pubsub: PubSubSystem, x_api_key: Optional[str] = None):
    stats_data = await pubsub.get_stats()
    return stats_data
