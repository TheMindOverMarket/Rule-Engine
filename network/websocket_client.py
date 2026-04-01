import asyncio
import websockets
import websockets.exceptions
import json
from typing import Callable, Awaitable, Union

class WebSocketClient:
    def __init__(self, url: str):
        self.url = url
        self.connection = None

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
                print(f"Connected to {self.url}")
                return
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.InvalidStatus as e:
                if e.response.status_code == 429:
                    attempt += 1
                    delay = base_delay * (2 ** (attempt - 1))
                    print(f"Rate limited (429) connecting to {self.url}. Retrying in {delay}s (Attempt {attempt}/{max_retries})...")
                    await asyncio.sleep(delay)
                else:
                    print(f"Failed to connect to {self.url}: {e}")
                    raise
            except Exception as e:
                if attempt < max_retries - 1:
                     attempt += 1
                     delay = base_delay * (2 ** (attempt - 1))
                     print(f"Connection error to {self.url}: {e}. Retrying in {delay}s...")
                     await asyncio.sleep(delay)
                else:
                    print(f"Failed to connect to {self.url} after {max_retries} attempts: {e}")
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
            print(f"Error sending data to {self.url}: {e}")
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
                    print(f"Connecting to {self.url}...")
                    await self.connect()
                    # Reset retry count after successful connection
                    retry_count = 0
                
                print(f"Listening on {self.url}...")
                async for message in self.connection:
                    await callback(message)
                
                # If we get here, the server closed the connection cleanly (or we broke out)
                print("Connection closed by server. Reconnecting...")
                self.connection = None
                
            except asyncio.CancelledError:
                self.connection = None
                raise
            except websockets.exceptions.ConnectionClosed as e:
                print(f"WebSocket connection closed (code: {e.code}, reason: {e.reason}).")
                self.connection = None
            except Exception as e:
                print(f"Unexpected error while listening: {e}.")
                self.connection = None
            
            # Backoff before reconnecting
            delay = min(base_delay * (2 ** retry_count), max_delay)
            print(f"Reconnecting to {self.url} in {delay:.1f}s...")
            await asyncio.sleep(delay)
            retry_count += 1
