import json
import asyncio

import aiofiles

from config.logger import logger
from config.settings import settings


class AsyncCacheFile:
    _lock = asyncio.Lock()

    def __init__(self):
        self._cache = {}
        self._loaded = False
        self.CACHE_PATH = settings.CACHE_PATH

    async def load(self):
        if self._loaded:
            return

        async with self._lock:
            if self.CACHE_PATH.exists():
                try:
                    async with aiofiles.open(self.CACHE_PATH, mode='r') as f:
                        content = await f.read()
                        self._cache = json.loads(content) if content else {}
                except (json.JSONDecodeError, Exception) as e:
                    logger.error(f"Cache load error: {str(e)}")
                    self._cache = {}
            self._loaded = True

    async def save(self):
        async with self._lock:
            self.CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(self.CACHE_PATH, mode='w') as f:
                await f.write(json.dumps(self._cache))

    async def get(self, key: str):
        await self.load()
        return self._cache.get(key)

    async def set(self, key: str, value):
        await self.load()
        self._cache[key] = value
        await self.save()

    async def contains(self, key: str) -> bool:
        await self.load()
        return key in self._cache
