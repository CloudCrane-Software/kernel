"""watcher/sources.py — read-only views of the mandates / eval-gate repos.

The watcher never checks anything out: it fetches into the mounted clone and
reads trees through `git ls-tree` / `git show` against FETCH_HEAD, so the
clone's working copy stays untouched no matter how many cycles run. Both
sources are small Protocols so tests can substitute in-memory fakes.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Protocol


class SourceError(RuntimeError):
    """A git read failed (fetch error, missing path, non-zero exit)."""


class MandatesSource(Protocol):
    async def head_commit(self) -> str:  # fetch + resolve the tracked ref
        ...

    async def list_workorders(self) -> list[str]:  # repo-relative .md paths
        ...

    async def read_workorder(self, file_path: str) -> str:  # file text at FETCH_HEAD
        ...


class SuiteSource(Protocol):
    async def read_suite(self, name: str) -> str | None:  # None = not found
        ...


class GitRepo:
    """Thin async wrapper over the git CLI for one mounted clone."""

    def __init__(self, workdir: Path, ref: str, timeout: float) -> None:
        self._workdir = workdir
        self._ref = ref
        self._timeout = timeout

    async def _git(self, *args: str) -> bytes:
        # safe.directory: the clone is owned by the host user, the container
        # may run under a different uid — without this git refuses to operate.
        argv = ("git", "-C", str(self._workdir), "-c", f"safe.directory={self._workdir}", *args)
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:  # git binary missing
            raise SourceError(f"cannot spawn git: {exc}") from exc
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self._timeout)
        except TimeoutError as exc:
            proc.kill()
            raise SourceError(f"git {args[0]} timed out after {self._timeout}s") from exc
        if proc.returncode != 0:
            detail = stderr.decode("utf-8", "replace").strip()
            raise SourceError(f"git {' '.join(args[:2])} failed ({proc.returncode}): {detail}")
        return stdout

    async def fetch_head(self) -> str:
        """Fetch the tracked ref and return the fetched commit sha."""
        await self._git("fetch", "--quiet", "origin", self._ref)
        out = await self._git("rev-parse", "FETCH_HEAD")
        sha = out.decode("utf-8", "replace").strip()
        if not sha:
            raise SourceError("git rev-parse FETCH_HEAD returned nothing")
        return sha

    async def list_tree(self, subdir: str) -> list[str]:
        out = await self._git("ls-tree", "-r", "--name-only", "FETCH_HEAD", "--", subdir)
        return [line for line in out.decode("utf-8", "replace").splitlines() if line.strip()]

    async def show(self, path: str) -> str:
        out = await self._git("show", f"FETCH_HEAD:{path}")
        return out.decode("utf-8", "replace")


class GitMandatesSource:
    """MandatesSource over a mounted clone; only direct children of the
    workorders directory that end in .md are considered work orders."""

    def __init__(self, workdir: Path, ref: str, timeout: float, prefix: str) -> None:
        self._repo = GitRepo(workdir, ref, timeout)
        self._prefix = prefix.strip("/") + "/"

    async def head_commit(self) -> str:
        return await self._repo.fetch_head()

    async def list_workorders(self) -> list[str]:
        files = await self._repo.list_tree(self._prefix)
        out: list[str] = []
        for path in files:
            if not path.startswith(self._prefix) or not path.endswith(".md"):
                continue
            rest = path[len(self._prefix) :]
            if "/" in rest:  # nested directories are not work orders
                continue
            out.append(path)
        return sorted(out)

    async def read_workorder(self, file_path: str) -> str:
        return await self._repo.show(file_path)


class GitSuiteSource:
    """SuiteSource over a mounted eval-gate clone; missing suites resolve to
    None (the dispatch file carries the reference plus a note, not a failure)."""

    def __init__(self, workdir: Path, ref: str, timeout: float) -> None:
        self._repo = GitRepo(workdir, ref, timeout)

    async def read_suite(self, name: str) -> str | None:
        try:
            return await self._repo.show(f"suites/{name}.md")
        except SourceError:
            return None
