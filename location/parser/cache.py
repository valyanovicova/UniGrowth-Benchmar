import json
import asyncio
from pathlib import Path

import aiofiles

from logger import logger


class AsyncCacheFile:
    CACHE_FILE = Path("geo_cache.json")
    _lock = asyncio.Lock()

    def __init__(self):
        self._cache = {}
        self._loaded = False

    async def load(self):
        if self._loaded:
            return

        async with self._lock:
            if self.CACHE_FILE.exists():
                try:
                    async with aiofiles.open(self.CACHE_FILE, mode='r') as f:
                        content = await f.read()
                        self._cache = json.loads(content) if content else {}
                except (json.JSONDecodeError, Exception) as e:
                    logger.error(f"Cache load error: {str(e)}")
                    self._cache = {}
            self._loaded = True

    async def save(self):
        async with self._lock:
            async with aiofiles.open(self.CACHE_FILE, mode='w') as f:
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