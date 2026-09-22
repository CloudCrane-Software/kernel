"""watcher/config.py — environment-driven settings (12-factor, pydantic-settings).

Every knob has a WATCHER_ prefixed env name; the gateway bearer token is the
one exception — it keeps the KERNEL_ADMIN_TOKEN name the platform stack
already uses (WATCHER_GATEWAY_TOKEN accepted as an alias). The token is a
SecretStr: repr/log-safe by construction.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class WatcherSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WATCHER_", frozen=True, extra="ignore")

    mandates_dir: Path = Path("/work/mandates")  # mounted mandates clone (rw)
    evalgate_dir: Path | None = None  # mounted eval-gate clone; unset = refs only
    mandates_ref: str = "main"  # branch/ref fetched every cycle
    dispatch_dir: Path = Path("/work/dispatch")
    state_path: Path = Path("/work/state/state.json")

    gateway_url: str = "http://gateway:8000"
    gateway_token: SecretStr | None = Field(
        default=None,
        validation_alias=AliasChoices("WATCHER_GATEWAY_TOKEN", "KERNEL_ADMIN_TOKEN"),
    )

    poll_interval: float = 60.0  # seconds between cycles
    git_timeout: float = 120.0
    http_timeout: float = 20.0

    baseline_on_first_run: bool = True  # first cycle records history, dispatches nothing
    workorders_prefix: str = "workorders"
    max_attempts: int = 5  # per work order; then status=failed + CRITICAL alert
    backoff_base: float = 60.0  # seconds; next retry = now + base * 2**(attempts-1)

    alert_cmd: str | None = "/usr/local/bin/alert-notify.sh"  # "" disables
    log_level: str = "INFO"

    def resolved_alert_cmd(self) -> str | None:
        cmd = self.alert_cmd
        return cmd if cmd else None
