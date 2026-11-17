import argparse
import asyncio
import json
import logging
import sys

from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# Define configuration and log file locations.
DEFAULT_CONFIG = BASE_DIR / "config" / "config.json"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "monitor.log"


class JsonFormatter(logging.Formatter):
    """
    Convert log messages into structured JSON format.

    Each log record contains a timestamp, log level,
    message, and optional network monitoring details.
    """

    def format(self, record):
        """Format one log record as a JSON string."""

        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage()
        }

        optional_fields = [
            "target",
            "host",
            "port",
            "status",
            "latency_ms",
            "error"
        ]

        for field in optional_fields:
            if hasattr(record, field):
                log_data[field] = getattr(record, field)

        return json.dumps(log_data, ensure_ascii=False)


def setup_logging():
    """
    Configure console and rotating file logging.

    Log files are saved inside the logs directory.
    Old log files are rotated automatically.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("network_monitor")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = JsonFormatter()

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )

    file_handler.setFormatter(formatter)

    # Display the same logs in the terminal.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def load_config(config_path):
    """
    Load and validate the JSON configuration file.

    Args:
        config_path: Path to the configuration file.

    Returns:
        A validated configuration dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If configuration values are invalid.
        json.JSONDecodeError: If JSON syntax is incorrect.
    """

    with open(config_path, "r", encoding="utf-8") as file:
        config = json.load(file)

    interval = config.get("interval_seconds")
    concurrency = config.get("max_concurrency")
    checks = config.get("checks")

    if (
        not isinstance(interval, (int, float))
        or isinstance(interval, bool)
        or interval <= 0
    ):
        raise ValueError("interval_seconds must be positive")

    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency < 1
    ):
        raise ValueError("max_concurrency must be a positive integer")

    if not isinstance(checks, list) or not checks:
        raise ValueError("checks must be a non-empty list")

    for check in checks:

        if not isinstance(check, dict):
            raise ValueError("Each check must be an object")

        if not isinstance(check.get("name"), str) or not check["name"].strip():
            raise ValueError("Each check must have a valid name")

        if not isinstance(check.get("host"), str) or not check["host"].strip():
            raise ValueError("Each check must have a valid host")

        port = check.get("port")

        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("Port must be between 1 and 65535")

        timeout = check.get("timeout_seconds")

        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout <= 0
        ):
            raise ValueError("timeout_seconds must be positive")

    return config


async def check_port(check, semaphore, logger):
    """
    Check whether a TCP port accepts connections.

    asyncio.open_connection establishes a TCP connection.
    asyncio.wait_for limits the connection timeout.

    Args:
        check: Monitoring target configuration.
        semaphore: Limits concurrent network checks.
        logger: Application logger.

    Returns:
        A dictionary containing the monitoring result.
    """

    name = check["name"]
    host = check["host"]
    port = check["port"]
    timeout = check["timeout_seconds"]

    start_time = asyncio.get_running_loop().time()

    status = "DOWN"
    error = None
    writer = None

    async with semaphore:

        try:

            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout
            )

            status = "UP"

        except asyncio.TimeoutError:
            error = "Connection timed out"

        except ConnectionRefusedError:
            error = "Connection refused"

        except OSError as exc:
            error = f"{type(exc).__name__}: {exc}"

        finally:

            if writer is not None:
                writer.close()

                try:
                    await writer.wait_closed()
                except OSError:
                    pass

    latency = round(
        (asyncio.get_running_loop().time() - start_time) * 1000,
        2
    )

    result = {
        "target": name,
        "host": host,
        "port": port,
        "status": status,
        "latency_ms": latency,
        "error": error
    }

    logger.info(
        "TCP port check completed",
        extra=result
    )

    return result


async def run_monitor(config, logger):
    """
    Run network checks repeatedly at configured intervals.

    All checks are executed concurrently using asyncio.gather.
    A semaphore controls the maximum number of simultaneous checks.

    Args:
        config: Validated monitoring configuration.
        logger: Application logger.
    """

    interval = config["interval_seconds"]

    semaphore = asyncio.Semaphore(
        config["max_concurrency"]
    )

    logger.info("Network monitor started")

    while True:

        cycle_start = asyncio.get_running_loop().time()

        logger.info("Starting a new monitoring cycle")

        results = await asyncio.gather(
            *[
                check_port(check, semaphore, logger)
                for check in config["checks"]
            ]
        )

        up_count = sum(
            result["status"] == "UP"
            for result in results
        )

        down_count = len(results) - up_count

        logger.info(
            "Monitoring cycle completed",
            extra={
                "status": "UP" if down_count == 0 else "DEGRADED",
                "up_count": up_count,
                "down_count": down_count
            }
        )

        elapsed = asyncio.get_running_loop().time() - cycle_start

        await asyncio.sleep(
            max(0, interval - elapsed)
        )


async def main():
    """
    Load configuration and start the monitoring application.
    """

    parser = argparse.ArgumentParser(
        description="Asynchronous TCP Network Monitor"
    )

    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help="Path to the JSON configuration file"
    )

    args = parser.parse_args()

    logger = setup_logging()

    try:
        config = load_config(args.config)

        await run_monitor(config, logger)

    except asyncio.CancelledError:
        raise

    except (OSError, ValueError, json.JSONDecodeError) as exc:

        logger.error(
            f"Application error: {exc}"
        )

        raise


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nNetwork monitor stopped.")

    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Fatal error: {exc}")
        sys.exit(1)
