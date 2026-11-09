import asyncio
import time
from typing import Dict, Set
from fastapi import WebSocket

class InMemoryMQTTBroker:
    def __init__(self):
        # Mapeo: tópico -> conjunto de WebSockets suscriptos
        self.subscriptions: Dict[str, Set[WebSocket]] = {}
        self.total_published_count: int = 0

    def subscribe(self, topic: str, websocket: WebSocket):
        if topic not in self.subscriptions:
            self.subscriptions[topic] = set()
        self.subscriptions[topic].add(websocket)

    def _remove_subscriber(self, topic: str, websocket: WebSocket):
        subscribers = self.subscriptions.get(topic)
        if subscribers is None:
            return
        subscribers.discard(websocket)
        if not subscribers:
            del self.subscriptions[topic]

    def _unsubscribe_from_topic(self, websocket: WebSocket, topic: str):
        if websocket in self.subscriptions.get(topic, set()):
            self._remove_subscriber(topic, websocket)

    def _unsubscribe_everywhere(self, websocket: WebSocket):
        # Elimina el WebSocket de todos los tópicos al desconectarse
        for topic in list(self.subscriptions.keys()):
            self._remove_subscriber(topic, websocket)

    def unsubscribe(self, websocket: WebSocket, topic: str = None):
        if topic:
            self._unsubscribe_from_topic(websocket, topic)
        else:
            self._unsubscribe_everywhere(websocket)

    def _build_message(self, topic: str, payload: str) -> dict:
        return {
            "type": "mqtt_message",
            "topic": topic,
            "payload": payload,
            "timestamp": time.time()
        }

    async def _broadcast(self, subscribers, message_data: dict):
        # Difusión asíncrona concurrente a todos los suscriptores
        tasks = [sub.send_json(message_data) for sub in subscribers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def publish(self, topic: str, payload: str, sender: WebSocket = None) -> int:
        self.total_published_count += 1
        subscribers = self.subscriptions.get(topic, set())

        if not subscribers:
            return 0

        await self._broadcast(subscribers, self._build_message(topic, payload))
        return len(subscribers)

    def get_stats(self):
        return {
            "total_topics": len(self.subscriptions),
            "total_messages_published": self.total_published_count,
            "topics": {
                topic: len(subs) for topic, subs in self.subscriptions.items()
            }
        }

# Instancia global del broker MQTT en memoria
mqtt_broker = InMemoryMQTTBroker()