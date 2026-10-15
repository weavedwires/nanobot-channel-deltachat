"""Delta Chat channel implementation using the deltachat-rpc-client SDK."""

import asyncio
import shutil
from pathlib import Path
from typing import Any, Optional

from deltachat_rpc_client import DeltaChat, EventType, Rpc
from loguru import logger
from nanobot.bus.events import OutboundMessage
from nanobot.bus.outbound_events import ProgressEvent
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.paths import get_media_dir, get_runtime_subdir
from nanobot.config.schema import Base
from pydantic import Field

from .chat_stream import ChatStreamController

# Интервал (сек) между обновлениями стримового сообщения через editMessage.
STREAM_THROTTLE_S = 2.0

# Сколько ждать полной загрузки вложения после download_full_message.
# download_full_message() только ставит задачу в scheduler и возвращается сразу,
# поэтому готовность файла приходится ждать отдельно.
DOWNLOAD_WAIT_TIMEOUT_S = 30.0
DOWNLOAD_POLL_INTERVAL_S = 0.25

# ViewType-значения Delta Chat, которые считаем медиа и передаём в agent.
_MEDIA_VIEW_TYPES = {
    "Image",
    "Gif",
    "Sticker",
    "Audio",
    "Voice",
    "Video",
    "File",
}

# downloadState, при которых имеет смысл запрашивать догрузку.
_PENDING_DOWNLOAD_STATES = {"Available", "Failure"}


class DeltaChatConfig(Base):
    """Конфигурация для канала DeltaChat."""

    enabled: bool = False
    account_url: str = ""
    allow_from: list[str] = Field(default_factory=list)
    db_dir: str = ""  # По умолчанию: ~/.nanobot/dc_channel/
    display_name: str = "nanobot"
    streaming: bool = True


class DeltaChatChannel(BaseChannel):
    name = "deltachat"
    display_name = "Delta Chat"

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = DeltaChatConfig(**config)
        super().__init__(config, bus)

        self._rpc: Optional[Rpc] = None
        self._dc: Optional[DeltaChat] = None
        self._account = None
        self._account_id: Optional[int] = None
        self._running = False
        self._stream_controller: Optional[ChatStreamController] = None

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return DeltaChatConfig().model_dump(by_alias=True)

    async def start(self) -> None:
        """Запускает DeltaChat аккаунт и начинает слушать входящие сообщения."""
        self._running = True

        if not self.config.account_url:
            raise ValueError("Отсутствует accountUrl DeltaChat")

        try:
            if self.config.db_dir:
                accounts_dir = Path(self.config.db_dir).expanduser()
            else:
                accounts_dir = get_runtime_subdir("dc_channel")
            self._rpc = Rpc(accounts_dir=str(accounts_dir))
            self._rpc.start()
            self._dc = DeltaChat(self._rpc)

            accounts = self._dc.get_all_accounts()
            self._account = accounts[0] if accounts else self._dc.add_account()

            self._account_id = self._account.id
            self._stream_controller = ChatStreamController(
                create_message=self._create_dc_message,
                edit_message=self._edit_dc_message,
                throttle_s=STREAM_THROTTLE_S,
            )
            if not self._account.is_configured():
                self._account.set_config("bot", "1")
                if self.config.display_name:
                    self._account.set_config("displayname", self.config.display_name)
                self._account.add_transport_from_qr(self.config.account_url)

            self._account.bring_online()
            try:
                invite_link = self._account.get_qr_code()
                logger.info(f"Ссылка-приглашение для бота: {invite_link}")
            except Exception as qr_err:
                logger.warning(f"Не удалось получить invite-ссылку: {qr_err}")
            logger.info("Аккаунт DeltaChat успешно настроен")
            await self._start_event_loop()

        except Exception as e:
            logger.error(f"Ошибка при запуске DeltaChat канала: {e}")
            raise
        finally:
            self._running = False
            if self._rpc:
                self._rpc.close()
                self._rpc = None
            self._dc = None
            self._account = None
            self._account_id = None

    @staticmethod
    def _event_value(event: Any, key: str, default: Any = None) -> Any:
        if isinstance(event, dict):
            return event.get(key, default)
        return getattr(event, key, default)

    async def _start_event_loop(self) -> None:
        """Вечный цикл событий."""
        while self._running:
            try:
                if self._account is None:
                    logger.warning("DeltaChat аккаунт не инициализирован, завершаем event loop")
                    break
                event = await asyncio.to_thread(self._account.wait_for_event)
                await self._handle_event(event)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в цикле событий DeltaChat: {e}")
                await asyncio.sleep(5)

    async def _handle_event(self, event) -> None:
        kind = self._event_value(event, "kind")
        if kind == EventType.INCOMING_MSG:
            msg_id = self._event_value(event, "msg_id")
            if msg_id is None:
                logger.warning(f"INCOMING_MSG без msg_id: {event}")
                return
            if self._account is None:
                logger.warning("Получено входящее сообщение до инициализации аккаунта")
                return

            message = self._account.get_message_by_id(msg_id)
            snapshot = message.get_snapshot()

            if snapshot.is_info:
                return

            content = snapshot.text or ""
            media, media_parts = await self._download_message_media(msg_id, snapshot)
            if media_parts:
                content = "\n".join([content, *media_parts]) if content else "\n".join(media_parts)

            await self._handle_message(
                sender_id=str(snapshot.from_id),
                chat_id=str(snapshot.chat_id),
                content=content,
                media=media,
            )
        elif kind == EventType.INFO:
            logger.info(self._event_value(event, "msg", str(event)))
        elif kind == EventType.WARNING:
            logger.warning(self._event_value(event, "msg", str(event)))
        elif kind == EventType.ERROR:
            logger.error(self._event_value(event, "msg", str(event)))

        logger.info("Остановка канала DeltaChat...")

    async def _download_message_media(
        self, msg_id: int, snapshot: Any, *, add_failure_content: bool = False
    ) -> tuple[list[str], list[str]]:
        """Скачать вложение входящего сообщения.

        Возвращает ``(media_paths, content_parts)`` в стиле Telegram-канала:
        пути к локальным копиям файлов и текстовые пометки о них.
        Файл копируется в media-каталог канала, потому что blob-файлы Delta Chat
        живут в кэше аккаунта и могут быть удалены/переименованы.
        """
        if self._account is None:
            return [], []

        view_type = self._event_value(snapshot, "view_type", None)
        if view_type not in _MEDIA_VIEW_TYPES:
            return [], []

        try:
            snapshot = await self._ensure_downloaded(msg_id, snapshot)
        except Exception as e:
            logger.warning(f"Не удалось догрузить вложение сообщения {msg_id}: {e}")
            if add_failure_content:
                return [], [f"[{view_type}: download failed]"]
            return [], []

        src = self._event_value(snapshot, "file", None)
        if not src:
            logger.warning(f"Сообщение {msg_id} имеет viewType={view_type}, но без file")
            if add_failure_content:
                return [], [f"[{view_type}: download failed]"]
            return [], []

        src_path = Path(src)
        if not src_path.is_file():
            logger.warning(f"Файл вложения не найден на диске: {src}")
            if add_failure_content:
                return [], [f"[{view_type}: download failed]"]
            return [], []

        try:
            media_dir = get_media_dir("deltachat")
            file_name = self._event_value(snapshot, "file_name", None) or src_path.name
            dest = media_dir / f"dc_{msg_id}_{self._safe_filename(file_name)}"
            await asyncio.to_thread(shutil.copy2, str(src_path), str(dest))
        except Exception as e:
            logger.warning(f"Не удалось скопировать вложение сообщения {msg_id}: {e}")
            if add_failure_content:
                return [], [f"[{view_type}: download failed]"]
            return [], []

        logger.info(f"Скачано вложение сообщения {msg_id}: {dest}")
        return [str(dest)], [f"[{view_type}: {dest}]"]

    async def _ensure_downloaded(self, msg_id: int, snapshot: Any) -> Any:
        """Догрузить вложение через download_full_message, если оно не скачано.

        ``download_full_message()`` только помечает сообщение как ``InProgress``
        и будит scheduler — файла ещё нет. Поэтому после вызова ждём смены
        ``download_state`` на ``Done``, периодически перечитывая сообщение.
        """
        if self._rpc is None or self._account_id is None or self._account is None:
            return snapshot

        file_path = self._event_value(snapshot, "file", None)
        download_state = self._event_value(snapshot, "download_state", None)
        if file_path and download_state == "Done":
            return snapshot
        if download_state not in _PENDING_DOWNLOAD_STATES:
            return snapshot

        await asyncio.to_thread(
            self._rpc.download_full_message, self._account_id, msg_id
        )

        deadline = asyncio.get_running_loop().time() + DOWNLOAD_WAIT_TIMEOUT_S
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(DOWNLOAD_POLL_INTERVAL_S)
            fresh = self._reload_snapshot(msg_id)
            if fresh is None:
                # Core мог заменить pre-message новым сообщением с другим id.
                replaced = self._find_replacement(msg_id)
                if replaced is None:
                    continue
                msg_id, fresh = replaced
            state = self._event_value(fresh, "download_state", None)
            path = self._event_value(fresh, "file", None)
            if path and state == "Done":
                return fresh
            if state == "Failure":
                logger.warning(f"Загрузка вложения сообщения {msg_id} не удалась")
                return fresh

        logger.warning(f"Таймаут ожидания загрузки вложения сообщения {msg_id}")
        return self._reload_snapshot(msg_id) or snapshot

    def _reload_snapshot(self, msg_id: int) -> Any | None:
        """Перечитать сообщение по id; None, если его больше нет."""
        if self._account is None:
            return None
        try:
            return self._account.get_message_by_id(msg_id).get_snapshot()
        except Exception:
            return None

    def _find_replacement(self, msg_id: int) -> tuple[int, Any] | None:
        """Найти сообщение, заменившее исходное после полной загрузки.

        Core может заменить pre-message новым сообщением с другим id, указав
        старый в ``original_msg_id``.
        """
        if self._account is None:
            return None
        try:
            fresh_ids = self._account.get_fresh_messages()
        except Exception:
            return None
        for candidate in fresh_ids:
            try:
                snap = candidate.get_snapshot()
            except Exception:
                continue
            if self._event_value(snap, "original_msg_id", None) == msg_id:
                return int(candidate.id), snap
        return None

    @staticmethod
    def _safe_filename(name: str) -> str:
        """Оставить только безопасное имя файла."""
        base = Path(name).name
        cleaned = "".join(
            ch if ch.isalnum() or ch in "._- " else "_" for ch in base
        ).strip()
        return cleaned or "attachment"

    def _create_dc_message(self, chat_id: str, content: str) -> int:
        if self._account is None:
            raise RuntimeError("DeltaChat аккаунт не инициализирован")
        chat = self._account.get_chat_by_id(int(chat_id))
        msg = chat.send_text(content)
        logger.info(f"Сообщение отправлено в чат {chat_id}")
        return int(msg.id)

    def _edit_dc_message(self, chat_id: str, msg_id: int, content: str) -> None:
        if self._rpc is None or self._account_id is None:
            raise RuntimeError("DeltaChat канал не инициализирован")
        self._rpc.send_edit_request(self._account_id, msg_id, content)
        logger.info(f"Сообщение {msg_id} обновлено в чате {chat_id}")

    def _require_stream_controller(self) -> ChatStreamController:
        if self._stream_controller is None:
            raise RuntimeError("DeltaChat канал не инициализирован")
        return self._stream_controller

    async def send(self, msg: OutboundMessage) -> None:
        controller = self._require_stream_controller()
        event = msg.event
        if isinstance(event, ProgressEvent):
            if event.reasoning or event.reasoning_delta or event.reasoning_end:
                return
            kind = "tool" if event.tool_hint else "progress"
        else:
            kind = "answer"
        if not msg.content:
            return
        await controller.on_whole(msg.chat_id, kind, msg.content)

    async def send_delta(
        self,
        chat_id: str,
        delta: str,
        metadata: dict[str, Any] | None = None,
        *,
        stream_id: str | None = None,
        stream_end: bool = False,
        resuming: bool = False,
        merge_next: bool = False,
    ) -> None:
        controller = self._require_stream_controller()
        if delta:
            await controller.on_stream(chat_id, "answer", delta)
        if stream_end:
            await controller.on_stream_end(chat_id, "answer", merge_next=merge_next)

    async def send_reasoning_delta(
        self,
        chat_id: str,
        delta: str,
        metadata: dict[str, Any] | None = None,
        *,
        stream_id: str | None = None,
    ) -> None:
        controller = self._require_stream_controller()
        if delta:
            await controller.on_stream(chat_id, "reasoning", delta)

    async def send_reasoning_end(
        self,
        chat_id: str,
        metadata: dict[str, Any] | None = None,
        *,
        stream_id: str | None = None,
    ) -> None:
        controller = self._require_stream_controller()
        await controller.on_stream_end(chat_id, "reasoning")
