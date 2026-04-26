import aiohttp
import asyncio
from typing import Optional

class HTTPClient:
    _session: Optional[aiohttp.ClientSession] = None

    @classmethod
    async def get_session(cls) -> aiohttp.ClientSession:
        if cls._session is None or cls._session.closed:
            # We use a longer timeout for LLM and complex rule processing
            timeout = aiohttp.ClientTimeout(total=60)
            cls._session = aiohttp.ClientSession(timeout=timeout)
        return cls._session

    @classmethod
    async def close(cls):
        if cls._session and not cls._session.closed:
            await cls._session.close()
            cls._session = None

    @classmethod
    async def post(cls, url: str, **kwargs):
        session = await cls.get_session()
        return await session.post(url, **kwargs)

    @classmethod
    async def get(cls, url: str, **kwargs):
        session = await cls.get_session()
        return await session.get(url, **kwargs)

    @classmethod
    async def patch(cls, url: str, **kwargs):
        session = await cls.get_session()
        return await session.patch(url, **kwargs)
