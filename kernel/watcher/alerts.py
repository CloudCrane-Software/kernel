"""watcher/alerts.py — best-effort alerting through the ops alert gateway.

Every alert funnel goes through alert-notify.sh (platform repo ops/scripts),
the same single notification gateway the rest of the alert layer uses. The
watcher treats alert delivery as advisory: a broken alert channel must never
take down the poll loop, so send() swallows and logs everything.
"""

from __future__ import annotations

import asyncio
import logging
import shlex
from typing import Protocol

logger = logging.getLogger(__name__)


class AlertSink(Protocol):
    async def send(self, severity: str, message: str) -> None: ...


class NullAlert:
    """Disabled alerting (WATCHER_ALERT_CMD='')."""

    async def send(self, severity: str, message: str) -> None:
        logger.info("alert(%s suppressed): %s", severity, message)


class CommandAlert:
    """Runs `<alert_cmd> <SEVERITY> <message>` without ever raising."""

    def __init__(self, command: str, timeout: float = 15.0) -> None:
        self._argv = shlex.split(command)
        self._timeout = timeout

    async def send(self, severity: str, message: str) -> None:
        if not self._argv:
            return
        single = " ".join(message.splitlines()).strip() or "watcher alert"
        try:
            proc = await asyncio.create_subprocess_exec(
                *self._argv,
                severity,
                single,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except OSError as exc:
            logger.warning("alert command %s could not start: %s", self._argv[0], exc)
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=self._timeout)
        except TimeoutError:
            proc.kill()
            logger.warning("alert command %s timed out", self._argv[0])
