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

    def unsubscribe(self, websocket: WebSocket, topic: str = None):
        if topic:
            if topic in self.subscriptions and websocket in self.subscriptions[topic]:
                self.subscriptions[topic].remove(websocket)
                if not self.subscriptions[topic]:
                    del self.subscriptions[topic]
        else:
            # Elimina el WebSocket de todos los tópicos al desconectarse
            for t in list(self.subscriptions.keys()):
                self.subscriptions[t].discard(websocket)
                if not self.subscriptions[t]:
                    del self.subscriptions[t]

    async def publish(self, topic: str, payload: str, sender: WebSocket = None) -> int:
        self.total_published_count += 1
        subscribers = self.subscriptions.get(topic, set())

        if not subscribers:
            return 0

        message_data = {
            "type": "mqtt_message",
            "topic": topic,
            "payload": payload,
            "timestamp": time.time()
        }

        # Difusión asíncrona concurrente a todos los suscriptores
        tasks = [sub.send_json(message_data) for sub in subscribers]
        await asyncio.gather(*tasks, return_exceptions=True)
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