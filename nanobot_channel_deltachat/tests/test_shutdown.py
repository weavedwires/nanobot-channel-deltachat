"""Регрессия: остановка канала обязана освобождать поток, ждущий событие.

Симптом до фикса: gateway не завершался по SIGTERM, systemd писал
``State 'stop-sigterm' timed out. Killing.`` Поток
``asyncio.to_thread(account.wait_for_event)`` навсегда оставался в ``queue.get()``
(отмена asyncio-задачи блокирующий вызов не прерывает), а ``asyncio.run()`` на
выходе ждал default executor до ``THREAD_JOIN_TIMEOUT`` = 300 с.
"""

import asyncio
import contextlib
import queue
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from ..runtime import DeltaChatChannel

# Корень форка nanobot: tests живут в <fork>/nanobot/channels/deltachat/tests/.
FORK_ROOT = Path(__file__).resolve().parents[4]

ACCOUNT_ID = 7

_SHUTDOWN_SCRIPT = textwrap.dedent(
    f"""
    import asyncio
    import queue
    import sys

    from nanobot.channels.deltachat.runtime import DeltaChatChannel


    class BlockingAccount:
        def __init__(self, events):
            self._events = events

        def wait_for_event(self):
            return self._events.get()


    class FakeRpc:
        def __init__(self):
            self.events = queue.Queue()

        def get_queue(self, account_id):
            return self.events

        def close(self):
            pass


    async def main():
        channel = DeltaChatChannel({{"accountUrl": "DCACCOUNT://x.org"}}, bus=None)
        rpc = FakeRpc()
        channel._rpc = rpc
        channel._account = BlockingAccount(rpc.events)
        channel._account_id = {ACCOUNT_ID}
        channel._running = True

        task = asyncio.create_task(channel._start_event_loop())
        await asyncio.sleep(0.1)
        if sys.argv[1] == "stop":
            await channel.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        print("shutdown done", flush=True)


    asyncio.run(main())
    print("process exit ok", flush=True)
    """
)


class BlockingAccount:
    """Аккаунт-заглушка: ``wait_for_event`` блокируется так же, как в SDK."""

    def __init__(self, events: queue.Queue) -> None:
        self._events = events
        self.calls = 0

    def wait_for_event(self):
        self.calls += 1
        return self._events.get()


class FakeRpc:
    """Заглушка ``deltachat_rpc_client.Rpc``: очередь событий аккаунта + close()."""

    def __init__(self) -> None:
        self.events: queue.Queue = queue.Queue()
        self.closed = False

    def get_queue(self, account_id: int) -> queue.Queue:
        assert account_id == ACCOUNT_ID
        return self.events

    def close(self) -> None:
        self.closed = True


def _channel_with_fake_rpc() -> tuple[DeltaChatChannel, FakeRpc, BlockingAccount]:
    channel = DeltaChatChannel({"accountUrl": "DCACCOUNT://x.org"}, bus=None)
    rpc = FakeRpc()
    account = BlockingAccount(rpc.events)
    channel._rpc = rpc  # type: ignore[assignment]
    channel._account = account  # type: ignore[assignment]
    channel._account_id = ACCOUNT_ID
    return channel, rpc, account


async def test_stop_unblocks_wait_for_event() -> None:
    """stop() будит цикл событий: задача завершается сама, отмена не нужна.

    Ждём дедлайном, а не ``asyncio.wait_for``: последний не бросает TimeoutError,
    если отменённая корутина проглотила CancelledError и вернулась нормально
    (``asyncio.timeouts``), поэтому на непатченном коде тест был бы ложно-зелёным
    и оставлял бы поток в ``queue.get()`` до конца тестовой сессии.
    """
    channel, rpc, account = _channel_with_fake_rpc()
    channel._running = True
    loop_task = asyncio.create_task(channel._start_event_loop())
    await asyncio.sleep(0.1)  # цикл ушёл в блокирующий wait_for_event
    assert account.calls == 1
    assert not loop_task.done()

    await channel.stop()

    try:
        deadline = time.monotonic() + 5.0
        while not loop_task.done() and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        assert loop_task.done(), "stop() не разбудил поток, застрявший в wait_for_event"
    finally:
        if not loop_task.done():
            loop_task.cancel()
            rpc.events.put(None)  # не оставляем поток в queue.get() на teardown лупа
            with contextlib.suppress(asyncio.CancelledError):
                await loop_task

    assert channel._running is False


async def test_stop_without_rpc_is_noop() -> None:
    """stop() до старта канала не должен падать."""
    channel = DeltaChatChannel({"accountUrl": "DCACCOUNT://x.org"}, bus=None)
    await channel.stop()
    assert channel._running is False


@pytest.mark.parametrize("mode", ["stop", "cancel-only"])
def test_process_exits_after_channel_shutdown(mode: str) -> None:
    """Процесс выходит сам: до фикса asyncio.run() висел на default executor.

    Повторяется путь ChannelManager: ``stop()`` (для mode="stop") →
    ``task.cancel()`` → ``await task`` → выход из ``asyncio.run()``.
    Без фикса ждущий поток остаётся в ``queue.get()`` и интерпретатор висит
    до ``THREAD_JOIN_TIMEOUT`` = 300 с, поэтому subprocess не укладывается
    в 25 с.
    """
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _SHUTDOWN_SCRIPT, mode],
            cwd=str(FORK_ROOT),
            capture_output=True,
            text=True,
            timeout=25,
        )
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(
            "процесс не завершился за 25 с: поток остался в wait_for_event, "
            "asyncio.run() ждёт shutdown_default_executor (до 300 с)"
        ) from exc
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert "shutdown done" in proc.stdout, proc.stdout[-2000:]
    assert "process exit ok" in proc.stdout, proc.stdout[-2000:]
