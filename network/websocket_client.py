import asyncio
import websockets
import websockets.exceptions
import json
import random
import time
from typing import Callable, Awaitable, Union

class WebSocketClient:
    def __init__(self, url: str):
        self.url = url
        self.connection = None
        self._last_success_time = 0

    def _get_jittered_delay(self, base_delay: float) -> float:
        """Adds +/- 20% jitter to the delay."""
        jitter = base_delay * 0.2
        return base_delay + random.uniform(-jitter, jitter)

    async def connect(self, max_retries=5, base_delay=2.0, **kwargs):
        """Establishes connection to the WebSocket URL with exponential backoff retry."""
        connect_args = {
            "open_timeout": 20,
        }
        connect_args.update(kwargs)

        attempt = 0
        while attempt < max_retries:
            try:
                self.connection = await websockets.connect(self.url, **connect_args)
                print(f"[WS][SUCCESS] Connected to {self.url}")
                self._last_success_time = time.time()
                return
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.InvalidStatus as e:
                # Specific handling for Rate Limiting
                if e.response.status_code == 429:
                    attempt += 1
                    # Start with a much higher base for 429
                    current_base = max(base_delay, 10.0)
                    delay = self._get_jittered_delay(current_base * (2 ** (attempt - 1)))
                    print(f"[WS][429] Rate limited connecting to {self.url}. Retrying in {delay:.1f}s (Attempt {attempt}/{max_retries})...")
                    await asyncio.sleep(delay)
                else:
                    print(f"[WS][ERROR] Failed to connect to {self.url} (Status: {e.response.status_code}): {e}")
                    raise
            except Exception as e:
                if attempt < max_retries - 1:
                     attempt += 1
                     delay = self._get_jittered_delay(base_delay * (2 ** (attempt - 1)))
                     print(f"[WS][RETRY] Connection error to {self.url}: {e}. Retrying in {delay:.1f}s...")
                     await asyncio.sleep(delay)
                else:
                    print(f"[WS][CRITICAL] Failed to connect to {self.url} after {max_retries} attempts: {e}")
                    raise

    async def send(self, data: Union[str, dict]):
        """Sends data to the websocket server."""
        if not self.connection:
            await self.connect()
        
        if isinstance(data, dict):
            message = json.dumps(data)
        else:
            message = str(data)

        try:
            await self.connection.send(message)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[WS][SEND_ERROR] Error sending data to {self.url}: {e}")
            raise

    async def listen(self, callback: Callable[[str], Awaitable[None]]):
        """
        Listens for messages and calls the callback for each message.
        Automatically reconnects if the connection is dropped with exponential backoff.
        """
        base_delay = 2.0
        max_delay = 60.0
        retry_count = 0
        
        while True:
            try:
                # connect() handles its own retry logic for initial connection
                if not self.connection:
                    print(f"[WS][LISTEN] Initiating connection to {self.url}...")
                    await self.connect()
                    # Only reset retry count if the connection was stable for at least 30 seconds
                    # This prevents resetting the backoff if we are in a rapid "connect-then-immediately-drop" loop
                    if time.time() - self._last_success_time > 30:
                        retry_count = 0
                
                print(f"[WS][LISTEN] Active on {self.url}")
                async for message in self.connection:
                    await callback(message)
                
                # If we get here, the server closed the connection cleanly
                print(f"[WS][CLOSE] Connection closed by server {self.url}. Reconnecting...")
                self.connection = None
                
            except asyncio.CancelledError:
                self.connection = None
                raise
            except websockets.exceptions.ConnectionClosed as e:
                print(f"[WS][CLOSE] WebSocket connection closed for {self.url} (code: {e.code}, reason: {e.reason}).")
                self.connection = None
            except Exception as e:
                print(f"[WS][ERROR] Unexpected error while listening to {self.url}: {e}")
                self.connection = None
            
            # Backoff before reconnecting
            # If the connection was very short-lived, we don't reset retry_count above,
            # so the delay continues to grow.
            delay = self._get_jittered_delay(min(base_delay * (2 ** retry_count), max_delay))
            print(f"[WS][RECONNECT] Waiting {delay:.1f}s before reconnecting to {self.url} (retry_count: {retry_count})...")
            await asyncio.sleep(delay)
            retry_count += 1
