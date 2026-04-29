# app/utils/pubsub_utils.py
import json
from typing import Any, Dict
from google.cloud import pubsub_v1
from app.utils.config import Config
import logging

config = Config()
logger = logging.getLogger(__name__)

def publish_to_pubsub(topic: str, message: Dict[str, Any]) -> None:
    """
    Publish a JSON message to a Pub/Sub topic.

    Args:
        topic (str): The name of the Pub/Sub topic to publish to.
        message (Dict[str, Any]): The message to publish, which will be serialized to JSON.

    Returns:
        None

    Raises:
        Exception: If there is an error publishing the message.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(config.SERVICE_PROJECT_ID, topic)
    message_json = json.dumps(message).encode('utf-8')
    try:
        publisher.publish(topic_path, message_json).result()
        logger.info(f"Message published to {topic}: {message}")
    except Exception as e:
        logger.error(f"Error publishing to {topic}: {str(e)}")
        # Log error but don't crash the whole process
        pass