
import asyncio
import json
import logging
import tempfile
import unittest

from pathlib import Path
from src.monitor import load_config, check_port


class TestNetworkMonitor(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.logger = logging.getLogger("test_monitor")

    def test_load_valid_config(self):
        config = {
            "interval_seconds": 60,
            "max_concurrency": 10,
            "checks": [
                {
                    "name": "HTTP",
                    "host": "127.0.0.1",
                    "port": 80,
                    "timeout_seconds": 3
                }
            ]
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")

            result = load_config(path)

        self.assertEqual(result["interval_seconds"], 60)
        self.assertEqual(len(result["checks"]), 1)

    def test_invalid_config(self):
        config = {
            "interval_seconds": 0,
            "max_concurrency": 10,
            "checks": [
                {
                    "name": "HTTP",
                    "host": "127.0.0.1",
                    "port": 80,
                    "timeout_seconds": 3
                }
            ]
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")

            with self.assertRaises(ValueError):
                load_config(path)

    async def test_open_port(self):
        async def handle_client(reader, writer):
            writer.close()

        server = await asyncio.start_server(
            handle_client, "127.0.0.1", 0
        )

        port = server.sockets[0].getsockname()[1]

        check = {
            "name": "Test Server",
            "host": "127.0.0.1",
            "port": port,
            "timeout_seconds": 3
        }

        try:
            result = await check_port(
                check, asyncio.Semaphore(1), self.logger
            )
        finally:
            server.close()
            await server.wait_closed()

        self.assertEqual(result["status"], "UP")

    async def test_closed_port(self):
        temp_server = await asyncio.start_server(
            lambda reader, writer: None,
            "127.0.0.1", 0
        )

        port = temp_server.sockets[0].getsockname()[1]

        temp_server.close()
        await temp_server.wait_closed()

        check = {
            "name": "Closed Port",
            "host": "127.0.0.1",
            "port": port,
            "timeout_seconds": 3
        }

        result = await check_port(
            check, asyncio.Semaphore(1), self.logger
        )

        self.assertEqual(result["status"], "DOWN")


if __name__ == "__main__":
    unittest.main()

