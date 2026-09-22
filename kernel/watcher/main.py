"""watcher/main.py — resident process entrypoint (python -m kernel.watcher.main).

Not a web service: no uvicorn. A plain async loop — one cycle, sleep,
repeat — with per-cycle crash isolation (an error alerts and retries on the
next cycle instead of killing the process) and fail-fast startup when
configuration is wrong.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

from kernel.watcher import __version__
from kernel.watcher.alerts import AlertSink, CommandAlert, NullAlert
from kernel.watcher.config import WatcherSettings
from kernel.watcher.dispatch import DispatchWriter
from kernel.watcher.gateway import GatewayClient
from kernel.watcher.poller import Watcher
from kernel.watcher.sources import GitMandatesSource, GitSuiteSource
from kernel.watcher.state import StateStore

logger = logging.getLogger("kernel.watcher")


def build_alert(settings: WatcherSettings) -> AlertSink:
    cmd = settings.resolved_alert_cmd()
    return CommandAlert(cmd) if cmd else NullAlert()


def build_watcher(settings: WatcherSettings) -> tuple[Watcher, GatewayClient, AlertSink]:
    mandates = GitMandatesSource(
        Path(settings.mandates_dir),
        settings.mandates_ref,
        settings.git_timeout,
        settings.workorders_prefix,
    )
    suites = (
        GitSuiteSource(Path(settings.evalgate_dir), settings.mandates_ref, settings.git_timeout)
        if settings.evalgate_dir is not None
        else None
    )
    gateway = GatewayClient(
        settings.gateway_url,
        settings.gateway_token.get_secret_value() if settings.gateway_token else None,
        timeout=settings.http_timeout,
    )
    alert = build_alert(settings)
    watcher = Watcher(
        settings=settings,
        mandates=mandates,
        gateway=gateway,
        store=StateStore(Path(settings.state_path)),
        writer=DispatchWriter(Path(settings.dispatch_dir)),
        alert=alert,
        suites=suites,
    )
    return watcher, gateway, alert


async def run_forever(
    settings: WatcherSettings,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Run cycles until cancelled. `sleep` is injectable for tests."""
    watcher, gateway, alert = build_watcher(settings)
    logger.info(
        "kernel watcher %s started (mandates=%s ref=%s gateway=%s interval=%.0fs)",
        __version__,
        settings.mandates_dir,
        settings.mandates_ref,
        settings.gateway_url,
        settings.poll_interval,
    )
    try:
        while True:
            try:
                result = await watcher.run_cycle()
                if result.baselined or result.dispatched or result.resumed or result.failed:
                    logger.info(
                        "cycle: seen=%d baselined=%d dispatched=%d resumed=%d skipped=%d failed=%d",
                        result.files_seen,
                        result.baselined,
                        result.dispatched,
                        result.resumed,
                        result.skipped,
                        result.failed,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("cycle crashed; alerting and retrying next cycle")
                await alert.send("CRITICAL", "watcher: poll cycle crashed (see watcher logs)")
            await sleep(settings.poll_interval)
    finally:
        await gateway.aclose()


async def serve(settings: WatcherSettings) -> None:
    """run_forever as a task, stopped cleanly by SIGINT/SIGTERM."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows fallback below
            loop.add_signal_handler(sig, loop.call_soon_threadsafe, stop.set)
    task = asyncio.create_task(run_forever(settings), name="watcher-loop")
    await stop.wait()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def main() -> int:
    try:
        settings = WatcherSettings()  # fail fast on bad config
    except Exception as exc:
        logging.basicConfig(level="ERROR")
        logger.error("invalid watcher configuration: %s", exc)
        return 2
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(serve(settings))
    return 0


if __name__ == "__main__":
    sys.exit(main())
