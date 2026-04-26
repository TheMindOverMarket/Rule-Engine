import asyncio
import json
import random
from typing import Dict, Set, Callable, Awaitable, Optional
from network.websocket_client import WebSocketClient

class MarketDataHub:
    """
    Manages shared WebSocket connections to reduce redundant upstream connections.
    Allows multiple subscribers to listen to the same stream.
    """
    _instances: Dict[str, 'MarketDataHub'] = {}

    def __init__(self, url: str):
        self.url = url
        self.client = WebSocketClient(url)
        self.subscribers: Set[Callable[[str], Awaitable[None]]] = set()
        self.is_listening = False
        self._listen_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    @classmethod
    async def get_instance(cls, url: str) -> 'MarketDataHub':
        """Returns a singleton instance for a given URL."""
        if url not in cls._instances:
            cls._instances[url] = MarketDataHub(url)
        return cls._instances[url]

    async def subscribe(self, callback: Callable[[str], Awaitable[None]]):
        """Registers a callback to receive messages from this stream."""
        async with self._lock:
            self.subscribers.add(callback)
            if not self.is_listening:
                print(f"[HUB] Starting new upstream connection to {self.url}...")
                self.is_listening = True
                self._listen_task = asyncio.create_task(self._run_listen_safe())
            else:
                print(f"[HUB] Multiplexing: Reusing existing connection to {self.url} for new subscriber.")

    async def _run_listen_safe(self):
        """Wrapper to ensure is_listening is reset if the task dies or is cancelled."""
        try:
            # Staggered start to avoid thundering herd across different Hub instances
            stagger = random.uniform(0.1, 2.0)
            await asyncio.sleep(stagger)
            
            await self.client.listen(self._distribute)
        except Exception as e:
            print(f"[HUB ERROR] Upstream listen task for {self.url} failed: {e}")
        finally:
            async with self._lock:
                print(f"[HUB] Connection loop for {self.url} has exited. Resetting state.")
                self.is_listening = False
                self._listen_task = None

    async def unsubscribe(self, callback: Callable[[str], Awaitable[None]]):
        """Unregisters a callback. Stops the upstream connection if no subscribers remain."""
        async with self._lock:
            if callback in self.subscribers:
                self.subscribers.remove(callback)
            
            if not self.subscribers:
                if self.is_listening:
                    if self._listen_task:
                        self._listen_task.cancel()
                        try:
                            await self._listen_task
                        except asyncio.CancelledError:
                            pass
                    self.is_listening = False
                    self._listen_task = None
                
                # Close the underlying connection
                if self.client.connection:
                    await self.client.connection.close()
                    self.client.connection = None
                
                # Remove from global instances to allow garbage collection
                if self.url in self._instances:
                    print(f"[HUB] All subscribers gone for {self.url}. Purging Hub instance.")
                    del self._instances[self.url]

    async def _distribute(self, message: str):
        """Internal callback that broadcasts received messages to all subscribers."""
        if not self.subscribers:
            return

        # Parse JSON once here to avoid redundant parsing in every subscriber
        try:
            data = json.loads(message)
        except Exception as e:
            print(f"[HUB ERROR] Failed to parse message from {self.url}: {e}")
            return

        # Directly gather the coroutines. gather() handles scheduling them efficiently.
        tasks = [callback(data) for callback in list(self.subscribers)]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def send(self, data: dict):
        """Sends data through the shared connection (e.g., subscription requests)."""
        await self.client.send(data)
