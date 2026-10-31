"""Bounded process-group shutdown for the local experiment entrypoints."""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time


class StageInterrupted(BaseException):
    def __init__(self, signum: int):
        self.signum = signum
        super().__init__(f"stage interrupted by signal {signum}")


def supervise(command, *, cwd, stdout=None, stderr=None, env=None, timeout=None,
              cleanup_grace=75, receipt: Path | None = None):
    """Run one owned session; forward interrupts and wait for bounded cleanup.

    The cleanup grace permits existing runtime finally blocks to close their
    resources. It does not permit another inference or execution attempt.
    Commands, environment values, and subprocess output are not serialized here.
    """
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("process supervision requires the main thread")
    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    process = None
    started = time.monotonic()
    interrupted = None
    timed_out = False
    forced = False

    def interrupt(signum, frame):
        raise StageInterrupted(signum)

    def send(sig):
        if process is None:
            return
        if process.pid == os.getpgrp():
            raise RuntimeError("refusing to signal caller process group")
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass

    try:
        for sig in previous:
            signal.signal(sig, interrupt)
        process = subprocess.Popen(command, cwd=cwd, stdout=stdout, stderr=stderr,
                                   env=env, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
            return subprocess.CompletedProcess(command, code)
        except (StageInterrupted, KeyboardInterrupt, subprocess.TimeoutExpired) as exc:
            interrupted = getattr(exc, "signum", signal.SIGINT if isinstance(exc, KeyboardInterrupt) else None)
            timed_out = isinstance(exc, subprocess.TimeoutExpired)
            for sig in previous:
                signal.signal(sig, signal.SIG_IGN)
            send(signal.SIGTERM)
            try:
                process.wait(timeout=cleanup_grace)
            except subprocess.TimeoutExpired:
                forced = True
                send(signal.SIGKILL)
                process.wait(timeout=5)
            # A leader can exit while a same-session grandchild remains.
            send(signal.SIGKILL)
            raise
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        if receipt is not None:
            receipt.parent.mkdir(parents=True, exist_ok=True)
            value = {"process_group": process.pid if process else None,
                     "returncode": process.returncode if process else None,
                     "signal": interrupted, "timed_out": timed_out,
                     "forced_kill": forced, "elapsed_seconds": time.monotonic() - started,
                     "cleanup_grace_seconds": cleanup_grace, "automatic_retries": 0}
            temporary = receipt.with_name(receipt.name + ".tmp")
            temporary.write_text(json.dumps(value, indent=2) + "\n")
            temporary.replace(receipt)
