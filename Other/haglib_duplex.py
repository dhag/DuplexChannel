"""
HagLib Duplex - 双方向通信ライブラリ (Python版)

TCP/UDP/名前付きパイプによる双方向通信を提供します。
リクエスト/レスポンスパターンとプッシュ送信をサポート。

Usage:
    # サーバー
    server = TcpDuplexServer()
    server.on_received = lambda ch, msg: print(f"Received: {msg.payload_string}")
    await server.start(12345)

    # クライアント
    client = TcpDuplexClient("localhost", 12345)
    await client.connect()
    response = await client.send_and_receive("Hello!")
    print(response.payload_string)
"""

from __future__ import annotations
import asyncio
import struct
import socket
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from typing import (
    Optional, Callable, Dict, List, Tuple, Iterator, 
    Union, Any, Awaitable
)
from collections.abc import Iterable
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# メッセージ関連
# =============================================================================

class MessageType(IntEnum):
    """メッセージ種別"""
    PUSH = 0      # 一方的なプッシュ
    REQUEST = 1   # 応答を期待するリクエスト
    RESPONSE = 2  # リクエストへの応答


@dataclass
class DuplexMessage:
    """双方向通信メッセージ"""
    id: int = 0
    type: MessageType = MessageType.PUSH
    tag: str = ""
    payload: bytes = field(default_factory=bytes)

    @property
    def payload_string(self) -> Optional[str]:
        """ペイロードを文字列として取得"""
        return self.payload.decode('utf-8') if self.payload else None

    @payload_string.setter
    def payload_string(self, value: Optional[str]):
        """ペイロードを文字列で設定"""
        self.payload = value.encode('utf-8') if value else b''

    @classmethod
    def from_text(cls, text: str) -> DuplexMessage:
        """テキストからメッセージを作成"""
        msg = cls()
        msg.payload_string = text
        return msg

    @classmethod
    def from_bytes(cls, data: bytes) -> DuplexMessage:
        """バイト列からメッセージを作成"""
        return cls(payload=data)

    def to_typed_payload(self) -> TypedPayload:
        """
        TypedPayloadに変換
        
        ペイロードがTypedPayload形式でない場合は、
        テキストとして解釈してTypedPayloadに包む
        """
        if not self.payload or len(self.payload) == 0:
            return TypedPayload()
        
        try:
            return TypedPayload.from_message(self)
        except (ValueError, Exception):
            # TypedPayload形式でない場合、テキストとして扱う
            try:
                text = self.payload.decode("utf-8")
                return TypedPayload.from_text(text)
            except UnicodeDecodeError:
                # バイナリとして扱う
                return TypedPayload.from_binary(self.payload)


# =============================================================================
# パケットフォーマット
# =============================================================================

class DuplexPacket:
    """
    パケットフォーマット（バイナリ）

    ヘッダー (16バイト固定):
    [0-3]   Magic "DPX\n" (4バイト)
    [4]     Version (1バイト) = 1
    [5]     MessageType (1バイト)
    [6-9]   MessageId (4バイト, little-endian)
    [10-13] PayloadLength (4バイト, little-endian)
    [14-15] TagLength (2バイト, little-endian)

    ボディ:
    [16..]  Tag (可変長, UTF-8)
    [..]    Payload (可変長)
    """
    HEADER_SIZE = 16
    MAGIC = b'DPX\n'
    VERSION = 1

    @classmethod
    def serialize(cls, message: DuplexMessage) -> bytes:
        """メッセージをバイト配列にシリアライズ"""
        tag_bytes = message.tag.encode('utf-8') if message.tag else b''
        payload_bytes = message.payload or b''

        total_length = cls.HEADER_SIZE + len(tag_bytes) + len(payload_bytes)
        buffer = bytearray(total_length)

        # Magic
        buffer[0:4] = cls.MAGIC
        # Version
        buffer[4] = cls.VERSION
        # MessageType
        buffer[5] = message.type
        # MessageId (little-endian)
        struct.pack_into('<I', buffer, 6, message.id)
        # PayloadLength (little-endian)
        struct.pack_into('<I', buffer, 10, len(payload_bytes))
        # TagLength (little-endian)
        struct.pack_into('<H', buffer, 14, len(tag_bytes))

        # Tag
        if tag_bytes:
            buffer[cls.HEADER_SIZE:cls.HEADER_SIZE + len(tag_bytes)] = tag_bytes

        # Payload
        if payload_bytes:
            start = cls.HEADER_SIZE + len(tag_bytes)
            buffer[start:start + len(payload_bytes)] = payload_bytes

        return bytes(buffer)

    @classmethod
    def parse_header(cls, header: bytes) -> Optional[Tuple[MessageType, int, int, int]]:
        """
        ヘッダーを解析（16バイト必要）
        Returns: (type, message_id, payload_length, tag_length) or None
        """
        if not header or len(header) < cls.HEADER_SIZE:
            return None

        # Magic check
        if header[0:4] != cls.MAGIC:
            return None

        # Version check
        if header[4] != cls.VERSION:
            return None

        msg_type = MessageType(header[5])
        message_id = struct.unpack_from('<I', header, 6)[0]
        payload_length = struct.unpack_from('<I', header, 10)[0]
        tag_length = struct.unpack_from('<H', header, 14)[0]

        return msg_type, message_id, payload_length, tag_length

    @classmethod
    def parse_body(cls, msg_type: MessageType, message_id: int, 
                   body: bytes, tag_length: int) -> DuplexMessage:
        """ボディを解析してメッセージを構築"""
        message = DuplexMessage(
            type=msg_type,
            id=message_id
        )

        if tag_length > 0 and len(body) >= tag_length:
            message.tag = body[:tag_length].decode('utf-8')

        payload_start = tag_length
        if len(body) > payload_start:
            message.payload = body[payload_start:]

        return message


# =============================================================================
# インターフェース
# =============================================================================

class IDuplexChannel(ABC):
    """双方向通信チャネルのインターフェース"""

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """接続中かどうか"""
        pass

    @property
    @abstractmethod
    def id(self) -> str:
        """識別子"""
        pass

    # イベントハンドラ
    on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None
    on_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None

    @abstractmethod
    async def send(self, message: DuplexMessage) -> None:
        """プッシュ送信（応答なし）"""
        pass

    async def send_text(self, text: str) -> None:
        """プッシュ送信（文字列）"""
        await self.send(DuplexMessage.from_text(text))

    async def send_bytes(self, data: bytes) -> None:
        """プッシュ送信（バイナリ）"""
        await self.send(DuplexMessage.from_bytes(data))

    @abstractmethod
    async def send_and_receive(self, message: DuplexMessage) -> DuplexMessage:
        """リクエスト送信して応答を待つ"""
        pass

    async def send_and_receive_text(self, text: str) -> DuplexMessage:
        """リクエスト送信して応答を待つ（文字列）"""
        return await self.send_and_receive(DuplexMessage.from_text(text))

    @abstractmethod
    async def reply(self, request: DuplexMessage, response: DuplexMessage) -> None:
        """リクエストに応答する"""
        pass

    async def reply_text(self, request: DuplexMessage, text: str) -> None:
        """リクエストに応答する（文字列）"""
        await self.reply(request, DuplexMessage.from_text(text))

    @abstractmethod
    async def close(self) -> None:
        """切断"""
        pass


class IDuplexServer(ABC):
    """双方向通信サーバーのインターフェース"""

    @property
    @abstractmethod
    def is_listening(self) -> bool:
        """リッスン中かどうか"""
        pass

    @property
    @abstractmethod
    def clients(self) -> List[IDuplexChannel]:
        """接続中のクライアント一覧"""
        pass

    # イベントハンドラ
    on_client_connected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
    on_client_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
    on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None

    @abstractmethod
    async def start(self, port: int) -> None:
        """リッスン開始"""
        pass

    @abstractmethod
    async def broadcast(self, message: DuplexMessage) -> None:
        """全クライアントにブロードキャスト"""
        pass

    async def broadcast_text(self, text: str) -> None:
        """全クライアントにブロードキャスト（文字列）"""
        await self.broadcast(DuplexMessage.from_text(text))

    @abstractmethod
    async def stop(self) -> None:
        """サーバー停止"""
        pass


# =============================================================================
# TCP双方向通信チャネル
# =============================================================================

class TcpDuplexChannel(IDuplexChannel):
    """TCP双方向通信チャネル"""

    def __init__(self, reader: asyncio.StreamReader = None, 
                 writer: asyncio.StreamWriter = None,
                 channel_id: str = None):
        import uuid
        self._reader = reader
        self._writer = writer
        self._id = channel_id or uuid.uuid4().hex[:8]
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._send_lock = asyncio.Lock()
        self._next_message_id = 0
        self._receive_task: Optional[asyncio.Task] = None
        self._closed = False

        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None
        self.on_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None

    @property
    def is_connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    @property
    def id(self) -> str:
        return self._id

    async def connect(self, host: str, port: int) -> None:
        """サーバーに接続"""
        self._reader, self._writer = await asyncio.open_connection(host, port)
        self._start_receiving()

    def _start_receiving(self) -> None:
        """受信処理を開始"""
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self) -> None:
        """受信ループ"""
        try:
            while not self._closed and self._reader:
                # ヘッダー読み取り
                header = await self._read_exact(DuplexPacket.HEADER_SIZE)
                if not header:
                    break

                parsed = DuplexPacket.parse_header(header)
                if not parsed:
                    break

                msg_type, message_id, payload_length, tag_length = parsed

                # ボディ読み取り
                body_length = tag_length + payload_length
                body = b''
                if body_length > 0:
                    body = await self._read_exact(body_length)
                    if not body:
                        break

                message = DuplexPacket.parse_body(msg_type, message_id, body, tag_length)
                await self._handle_message(message)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Receive loop error: {e}")
        finally:
            if not self._closed:
                await self._notify_disconnected()

            # 保留中のリクエストをキャンセル
            for future in self._pending_requests.values():
                if not future.done():
                    future.cancel()
            self._pending_requests.clear()

    async def _read_exact(self, count: int) -> Optional[bytes]:
        """指定バイト数を正確に読み取る"""
        data = b''
        while len(data) < count:
            chunk = await self._reader.read(count - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    async def _handle_message(self, message: DuplexMessage) -> None:
        """メッセージを処理"""
        if message.type == MessageType.RESPONSE:
            # リクエストへの応答
            future = self._pending_requests.pop(message.id, None)
            if future and not future.done():
                future.set_result(message)
        else:
            # プッシュまたはリクエスト → イベント発火
            if self.on_received:
                try:
                    result = self.on_received(self, message)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.error(f"on_received handler error: {e}")

    async def _notify_disconnected(self) -> None:
        """切断通知"""
        if self.on_disconnected:
            try:
                result = self.on_disconnected(self)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_disconnected handler error: {e}")

    async def send(self, message: DuplexMessage) -> None:
        """プッシュ送信"""
        message.type = MessageType.PUSH
        self._next_message_id += 1
        message.id = self._next_message_id
        await self._send_internal(message)

    async def send_and_receive(self, message: DuplexMessage) -> DuplexMessage:
        """リクエスト送信して応答を待つ"""
        message.type = MessageType.REQUEST
        self._next_message_id += 1
        message.id = self._next_message_id

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_requests[message.id] = future

        try:
            await self._send_internal(message)
            return await future
        finally:
            self._pending_requests.pop(message.id, None)

    async def reply(self, request: DuplexMessage, response: DuplexMessage) -> None:
        """リクエストに応答"""
        response.type = MessageType.RESPONSE
        response.id = request.id
        await self._send_internal(response)

    async def _send_internal(self, message: DuplexMessage) -> None:
        """内部送信処理"""
        async with self._send_lock:
            packet = DuplexPacket.serialize(message)
            self._writer.write(packet)
            await self._writer.drain()

    async def close(self) -> None:
        """切断"""
        if self._closed:
            return

        self._closed = True

        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

        await self._notify_disconnected()


# =============================================================================
# TCPクライアント
# =============================================================================

class TcpDuplexClient(IDuplexChannel):
    """TCP双方向通信クライアント"""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._channel: Optional[TcpDuplexChannel] = None

        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None
        self.on_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None

    @property
    def is_connected(self) -> bool:
        return self._channel.is_connected if self._channel else False

    @property
    def id(self) -> str:
        return self._channel.id if self._channel else ""

    async def connect(self) -> None:
        """サーバーに接続"""
        if self._channel:
            raise RuntimeError("Already connected.")

        self._channel = TcpDuplexChannel()
        self._channel.on_received = self._on_received
        self._channel.on_disconnected = self._on_disconnected
        await self._channel.connect(self._host, self._port)

    async def reconnect(self) -> None:
        """再接続"""
        if self._channel:
            await self._channel.close()
            self._channel = None
        await self.connect()

    async def _on_received(self, ch: IDuplexChannel, msg: DuplexMessage) -> None:
        if self.on_received:
            await self.on_received(self, msg)

    async def _on_disconnected(self, ch: IDuplexChannel) -> None:
        if self.on_disconnected:
            await self.on_disconnected(self)

    async def send(self, message: DuplexMessage) -> None:
        self._ensure_connected()
        await self._channel.send(message)

    async def send_and_receive(self, message: DuplexMessage) -> DuplexMessage:
        self._ensure_connected()
        return await self._channel.send_and_receive(message)

    async def reply(self, request: DuplexMessage, response: DuplexMessage) -> None:
        self._ensure_connected()
        await self._channel.reply(request, response)

    async def close(self) -> None:
        if self._channel:
            await self._channel.close()

    def _ensure_connected(self) -> None:
        if not self._channel or not self._channel.is_connected:
            raise RuntimeError("Not connected.")


# =============================================================================
# TCPサーバー
# =============================================================================

class TcpDuplexServer(IDuplexServer):
    """TCP双方向通信サーバー"""

    def __init__(self):
        self._server: Optional[asyncio.Server] = None
        self._clients: Dict[str, TcpDuplexChannel] = {}
        self._client_id_counter = 0

        self.on_client_connected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
        self.on_client_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None

    @property
    def is_listening(self) -> bool:
        return self._server is not None and self._server.is_serving()

    @property
    def clients(self) -> List[IDuplexChannel]:
        return list(self._clients.values())

    async def start(self, port: int, host: str = '0.0.0.0') -> None:
        """リッスン開始"""
        if self._server:
            raise RuntimeError("Server is already running.")

        self._server = await asyncio.start_server(
            self._handle_connection, host, port
        )

    async def _handle_connection(self, reader: asyncio.StreamReader, 
                                  writer: asyncio.StreamWriter) -> None:
        """新規接続を処理"""
        self._client_id_counter += 1
        client_id = f"C{self._client_id_counter:04d}"

        channel = TcpDuplexChannel(reader, writer, client_id)
        channel.on_received = self._on_channel_received
        channel.on_disconnected = self._on_channel_disconnected

        self._clients[client_id] = channel
        channel._start_receiving()

        if self.on_client_connected:
            try:
                result = self.on_client_connected(channel)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_client_connected handler error: {e}")

    async def _on_channel_received(self, ch: IDuplexChannel, msg: DuplexMessage) -> None:
        if self.on_received:
            await self.on_received(ch, msg)

    async def _on_channel_disconnected(self, ch: IDuplexChannel) -> None:
        self._clients.pop(ch.id, None)
        if self.on_client_disconnected:
            try:
                result = self.on_client_disconnected(ch)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_client_disconnected handler error: {e}")

    async def broadcast(self, message: DuplexMessage) -> None:
        """全クライアントにブロードキャスト"""
        tasks = [client.send(message) for client in self._clients.values()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def broadcast_except(self, exclude_client_id: str, message: DuplexMessage) -> None:
        """特定のクライアント以外に送信"""
        tasks = [
            client.send(message) 
            for client in self._clients.values() 
            if client.id != exclude_client_id
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """サーバー停止"""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        # 全クライアント切断
        for client in list(self._clients.values()):
            await client.close()
        self._clients.clear()


# =============================================================================
# UDP双方向通信チャネル
# =============================================================================

class UdpDuplexChannel:
    """
    UDP双方向通信チャネル

    注意:
    - コネクションレス（接続状態なし）
    - パケットロス・順序逆転の可能性あり
    - 1パケット最大約1400バイト推奨（MTU制限）
    - send_and_receive は応答が届かない可能性あり（タイムアウト必須）
    """

    def __init__(self):
        import uuid
        self._id = uuid.uuid4().hex[:8]
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[_UdpProtocol] = None
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._send_lock = asyncio.Lock()
        self._next_message_id = 0
        self._remote_endpoint: Optional[Tuple[str, int]] = None
        self._last_received_from: Optional[Tuple[str, int]] = None

        self.on_received: Optional[Callable[[UdpDuplexChannel, DuplexMessage, Tuple[str, int]], Awaitable[None]]] = None

    @property
    def id(self) -> str:
        return self._id

    @property
    def last_received_from(self) -> Optional[Tuple[str, int]]:
        return self._last_received_from

    async def bind(self, port: int, host: str = '0.0.0.0') -> None:
        """受信用にバインド（サーバー的な使い方）"""
        loop = asyncio.get_event_loop()
        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _UdpProtocol(self),
            local_addr=(host, port)
        )

    async def connect(self, host: str, port: int) -> None:
        """送信先を設定（クライアント的な使い方）"""
        self._remote_endpoint = (host, port)
        if not self._transport:
            loop = asyncio.get_event_loop()
            # remote_addrを指定せずに作成（sendtoでアドレス指定可能にするため）
            self._transport, self._protocol = await loop.create_datagram_endpoint(
                lambda: _UdpProtocol(self),
                local_addr=('0.0.0.0', 0)  # エフェメラルポート
            )

    async def bind_and_connect(self, local_port: int, remote_host: str, remote_port: int) -> None:
        """バインドと送信先設定を両方行う"""
        loop = asyncio.get_event_loop()
        self._remote_endpoint = (remote_host, remote_port)
        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _UdpProtocol(self),
            local_addr=('0.0.0.0', local_port)
        )

    async def send(self, message: DuplexMessage, 
                   remote: Tuple[str, int] = None) -> None:
        """プッシュ送信"""
        remote = remote or self._remote_endpoint
        if not remote:
            raise RuntimeError("Remote endpoint not set. Call connect() first.")

        message.type = MessageType.PUSH
        self._next_message_id += 1
        message.id = self._next_message_id
        await self._send_internal(message, remote)

    async def send_text(self, text: str, remote: Tuple[str, int] = None) -> None:
        await self.send(DuplexMessage.from_text(text), remote)

    async def send_bytes(self, data: bytes, remote: Tuple[str, int] = None) -> None:
        await self.send(DuplexMessage.from_bytes(data), remote)

    async def send_and_receive(self, message: DuplexMessage, 
                                timeout_ms: int = 3000,
                                remote: Tuple[str, int] = None) -> DuplexMessage:
        """リクエスト送信して応答を待つ（タイムアウト必須）"""
        remote = remote or self._remote_endpoint
        if not remote:
            raise RuntimeError("Remote endpoint not set. Call connect() first.")

        message.type = MessageType.REQUEST
        self._next_message_id += 1
        message.id = self._next_message_id

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_requests[message.id] = future

        try:
            await self._send_internal(message, remote)
            return await asyncio.wait_for(future, timeout=timeout_ms / 1000)
        finally:
            self._pending_requests.pop(message.id, None)

    async def reply(self, request: DuplexMessage, response: DuplexMessage,
                    remote: Tuple[str, int]) -> None:
        """リクエストに応答"""
        response.type = MessageType.RESPONSE
        response.id = request.id
        await self._send_internal(response, remote)

    async def reply_text(self, request: DuplexMessage, text: str,
                         remote: Tuple[str, int]) -> None:
        await self.reply(request, DuplexMessage.from_text(text), remote)

    async def broadcast(self, message: DuplexMessage, port: int) -> None:
        """ブロードキャスト送信"""
        message.type = MessageType.PUSH
        self._next_message_id += 1
        message.id = self._next_message_id

        # ブロードキャスト許可
        sock = self._transport.get_extra_info('socket')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        await self._send_internal(message, ('<broadcast>', port))

    async def _send_internal(self, message: DuplexMessage, 
                              remote: Tuple[str, int]) -> None:
        async with self._send_lock:
            packet = DuplexPacket.serialize(message)

            if len(packet) > 1400:
                logger.warning(f"UDP packet size {len(packet)} exceeds recommended MTU")

            self._transport.sendto(packet, remote)

    def _handle_datagram(self, data: bytes, addr: Tuple[str, int]) -> None:
        """受信データを処理"""
        self._last_received_from = addr

        if len(data) < DuplexPacket.HEADER_SIZE:
            return

        parsed = DuplexPacket.parse_header(data[:DuplexPacket.HEADER_SIZE])
        if not parsed:
            return

        msg_type, message_id, payload_length, tag_length = parsed
        body_length = tag_length + payload_length
        body = data[DuplexPacket.HEADER_SIZE:DuplexPacket.HEADER_SIZE + body_length]

        message = DuplexPacket.parse_body(msg_type, message_id, body, tag_length)

        if message.type == MessageType.RESPONSE:
            future = self._pending_requests.pop(message.id, None)
            if future and not future.done():
                future.set_result(message)
        else:
            if self.on_received:
                asyncio.create_task(self._invoke_on_received(message, addr))

    async def _invoke_on_received(self, message: DuplexMessage, addr: Tuple[str, int]) -> None:
        try:
            result = self.on_received(self, message, addr)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"on_received handler error: {e}")

    def close(self) -> None:
        """切断"""
        if self._transport:
            self._transport.close()

        for future in self._pending_requests.values():
            if not future.done():
                future.cancel()
        self._pending_requests.clear()


class _UdpProtocol(asyncio.DatagramProtocol):
    """UDPプロトコルハンドラ"""

    def __init__(self, channel: UdpDuplexChannel):
        self._channel = channel

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        self._channel._handle_datagram(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.debug(f"UDP error: {exc}")


# =============================================================================
# TypedPayload (型付きペイロード)
# =============================================================================

class ContentType(IntEnum):
    """コンテンツ種別"""
    TEXT = 0
    BINARY = 1
    IMAGE = 2
    JSON = 3
    CUSTOM = 255


@dataclass
class TypedPayloadItem:
    """型付きペイロード（単一アイテム）"""
    type: ContentType = ContentType.TEXT
    mime_type: str = ""
    data: bytes = field(default_factory=bytes)

    @property
    def data_string(self) -> Optional[str]:
        """データを文字列として取得（Text/Json用）"""
        return self.data.decode('utf-8') if self.data else None

    @data_string.setter
    def data_string(self, value: Optional[str]):
        self.data = value.encode('utf-8') if value else b''

    # ファクトリメソッド
    @classmethod
    def text(cls, text: str) -> TypedPayloadItem:
        item = cls(type=ContentType.TEXT, mime_type="text/plain")
        item.data_string = text
        return item

    @classmethod
    def json(cls, json_str: str) -> TypedPayloadItem:
        item = cls(type=ContentType.JSON, mime_type="application/json")
        item.data_string = json_str
        return item

    @classmethod
    def image(cls, image_data: bytes, mime_type: str = "image/png") -> TypedPayloadItem:
        return cls(type=ContentType.IMAGE, mime_type=mime_type, data=image_data)

    @classmethod
    def image_auto(cls, image_data: bytes) -> TypedPayloadItem:
        return cls.image(image_data, detect_image_mime_type(image_data))

    @classmethod
    def binary(cls, data: bytes, mime_type: str = "application/octet-stream") -> TypedPayloadItem:
        return cls(type=ContentType.BINARY, mime_type=mime_type, data=data)

    @classmethod
    def custom(cls, data: bytes, mime_type: str) -> TypedPayloadItem:
        return cls(type=ContentType.CUSTOM, mime_type=mime_type, data=data)

    def serialize(self) -> bytes:
        """
        単一アイテムをシリアライズ
        形式: [ContentType:1][MimeLen:2][Mime...][Data...]
        """
        mime_bytes = self.mime_type.encode('utf-8') if self.mime_type else b''
        data_bytes = self.data or b''

        total_length = 3 + len(mime_bytes) + len(data_bytes)
        buffer = bytearray(total_length)

        buffer[0] = self.type
        struct.pack_into('<H', buffer, 1, len(mime_bytes))

        if mime_bytes:
            buffer[3:3 + len(mime_bytes)] = mime_bytes

        if data_bytes:
            start = 3 + len(mime_bytes)
            buffer[start:start + len(data_bytes)] = data_bytes

        return bytes(buffer)

    def __str__(self) -> str:
        return f'Item {{ Type={self.type.name}, Mime="{self.mime_type}", Size={len(self.data) if self.data else 0} }}'


def detect_image_mime_type(data: bytes) -> str:
    """画像データからMIMEタイプを検出"""
    if not data or len(data) < 4:
        return "application/octet-stream"

    if data[:4] == b'\x89PNG':
        return "image/png"
    if data[:3] == b'\xff\xd8\xff':
        return "image/jpeg"
    if data[:4] == b'GIF8':
        return "image/gif"
    if data[:2] == b'BM':
        return "image/bmp"
    if len(data) >= 12 and data[:4] == b'RIFF' and data[8:12] == b'WEBP':
        return "image/webp"

    return "application/octet-stream"


class TypedPayload(Iterable[TypedPayloadItem]):
    """
    型付きペイロードリスト（複数アイテム混在）

    シリアライズ形式:
    [0-3]    アイテム数 (4バイト, little-endian)
    各アイテム:
      [0-3]    アイテムサイズ (4バイト, little-endian)
      [4]      ContentType (1バイト)
      [5-6]    MimeType長 (2バイト, little-endian)
      [7..]    MimeType (UTF-8)
      [..]     Data
    """

    def __init__(self, items: List[TypedPayloadItem] = None):
        self._items: List[TypedPayloadItem] = items or []

    @property
    def count(self) -> int:
        return len(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, index: int) -> TypedPayloadItem:
        return self._items[index]

    def __iter__(self) -> Iterator[TypedPayloadItem]:
        return iter(self._items)

    # ファクトリメソッド
    @classmethod
    def from_text(cls, text: str) -> TypedPayload:
        return cls([TypedPayloadItem.text(text)])

    @classmethod
    def from_json(cls, json_str: str) -> TypedPayload:
        return cls([TypedPayloadItem.json(json_str)])

    @classmethod
    def from_image(cls, image_data: bytes, mime_type: str = "image/png") -> TypedPayload:
        return cls([TypedPayloadItem.image(image_data, mime_type)])

    @classmethod
    def from_binary(cls, data: bytes, mime_type: str = "application/octet-stream") -> TypedPayload:
        return cls([TypedPayloadItem.binary(data, mime_type)])

    @classmethod
    def from_custom(cls, data: bytes, mime_type: str) -> TypedPayload:
        return cls([TypedPayloadItem.custom(data, mime_type)])

    # アイテム操作（チェーン可能）
    def add(self, item: TypedPayloadItem) -> TypedPayload:
        self._items.append(item)
        return self

    def add_text(self, text: str) -> TypedPayload:
        return self.add(TypedPayloadItem.text(text))

    def add_json(self, json_str: str) -> TypedPayload:
        return self.add(TypedPayloadItem.json(json_str))

    def add_image(self, image_data: bytes, mime_type: str = "image/png") -> TypedPayload:
        return self.add(TypedPayloadItem.image(image_data, mime_type))

    def add_image_auto(self, image_data: bytes) -> TypedPayload:
        return self.add(TypedPayloadItem.image_auto(image_data))

    def add_binary(self, data: bytes, mime_type: str = "application/octet-stream") -> TypedPayload:
        return self.add(TypedPayloadItem.binary(data, mime_type))

    def add_custom(self, data: bytes, mime_type: str) -> TypedPayload:
        return self.add(TypedPayloadItem.custom(data, mime_type))

    def clear(self) -> None:
        self._items.clear()

    # 検索・取得
    def get_first(self, content_type: ContentType) -> Optional[TypedPayloadItem]:
        for item in self._items:
            if item.type == content_type:
                return item
        return None

    def get_first_by_mime(self, mime_type: str) -> Optional[TypedPayloadItem]:
        for item in self._items:
            if item.mime_type == mime_type:
                return item
        return None

    def get_all(self, content_type: ContentType) -> Iterator[TypedPayloadItem]:
        for item in self._items:
            if item.type == content_type:
                yield item

    def get_text(self) -> Optional[str]:
        item = self.get_first(ContentType.TEXT)
        return item.data_string if item else None

    def get_json(self) -> Optional[str]:
        item = self.get_first(ContentType.JSON)
        return item.data_string if item else None

    def get_image(self) -> Optional[bytes]:
        item = self.get_first(ContentType.IMAGE)
        return item.data if item else None

    def get_image_mime_type(self) -> Optional[str]:
        item = self.get_first(ContentType.IMAGE)
        return item.mime_type if item else None

    def get_binary(self) -> Optional[bytes]:
        item = self.get_first(ContentType.BINARY)
        return item.data if item else None

    def get_all_texts(self) -> Iterator[str]:
        for item in self.get_all(ContentType.TEXT):
            yield item.data_string

    def get_all_json(self) -> Iterator[str]:
        for item in self.get_all(ContentType.JSON):
            yield item.data_string

    def get_all_images(self) -> Iterator[Tuple[bytes, str]]:
        for item in self.get_all(ContentType.IMAGE):
            yield item.data, item.mime_type

    def get_all_binaries(self) -> Iterator[Tuple[bytes, str]]:
        for item in self.get_all(ContentType.BINARY):
            yield item.data, item.mime_type

    # シリアライズ
    def serialize(self) -> bytes:
        """バイト配列にシリアライズ"""
        serialized_items = [item.serialize() for item in self._items]
        total_size = 4 + sum(4 + len(item_bytes) for item_bytes in serialized_items)

        buffer = bytearray(total_size)
        offset = 0

        # アイテム数
        struct.pack_into('<I', buffer, offset, len(self._items))
        offset += 4

        # 各アイテム
        for item_bytes in serialized_items:
            # アイテムサイズ
            struct.pack_into('<I', buffer, offset, len(item_bytes))
            offset += 4
            # アイテムデータ
            buffer[offset:offset + len(item_bytes)] = item_bytes
            offset += len(item_bytes)

        return bytes(buffer)

    @classmethod
    def deserialize(cls, data: bytes) -> TypedPayload:
        """バイト配列からデシリアライズ"""
        if not data or len(data) < 4:
            raise ValueError("Invalid payload data: too short")

        payload = cls()
        offset = 0

        # アイテム数
        item_count = struct.unpack_from('<I', data, offset)[0]
        offset += 4

        for _ in range(item_count):
            if offset + 4 > len(data):
                raise ValueError("Invalid payload data: item size truncated")

            # アイテムサイズ
            item_size = struct.unpack_from('<I', data, offset)[0]
            offset += 4

            if offset + item_size > len(data):
                raise ValueError("Invalid payload data: item data truncated")

            # アイテムデータを抽出
            item_data = data[offset:offset + item_size]
            item = cls._deserialize_item(item_data)
            payload._items.append(item)

            offset += item_size

        return payload

    @staticmethod
    def _deserialize_item(data: bytes) -> TypedPayloadItem:
        if not data or len(data) < 3:
            raise ValueError("Invalid item data: too short")

        item = TypedPayloadItem()
        item.type = ContentType(data[0])
        mime_length = struct.unpack_from('<H', data, 1)[0]

        if len(data) < 3 + mime_length:
            raise ValueError("Invalid item data: mime type truncated")

        if mime_length > 0:
            item.mime_type = data[3:3 + mime_length].decode('utf-8')
        else:
            item.mime_type = ""

        data_start = 3 + mime_length
        if len(data) > data_start:
            item.data = data[data_start:]
        else:
            item.data = b''

        return item

    # DuplexMessage変換
    def to_message(self) -> DuplexMessage:
        return DuplexMessage(payload=self.serialize())

    @classmethod
    def from_message(cls, message: DuplexMessage) -> TypedPayload:
        if not message or not message.payload:
            return cls()
        return cls.deserialize(message.payload)

    def __str__(self) -> str:
        return f"TypedPayload {{ Count={len(self._items)} }}"


# =============================================================================
# TypedPayload用拡張関数
# =============================================================================

async def send_typed_payload(channel: IDuplexChannel, payload: TypedPayload) -> None:
    """TypedPayloadを送信"""
    await channel.send_bytes(payload.serialize())


async def send_typed_text(channel: IDuplexChannel, text: str) -> None:
    """テキストをTypedPayloadとして送信"""
    await channel.send_bytes(TypedPayload.from_text(text).serialize())


async def send_typed_json(channel: IDuplexChannel, json_str: str) -> None:
    """JSONをTypedPayloadとして送信"""
    await channel.send_bytes(TypedPayload.from_json(json_str).serialize())


async def send_typed_image(channel: IDuplexChannel, image_data: bytes, 
                            mime_type: str = "image/png") -> None:
    """画像をTypedPayloadとして送信"""
    await channel.send_bytes(TypedPayload.from_image(image_data, mime_type).serialize())


async def send_and_receive_typed(channel: IDuplexChannel, 
                                  payload: TypedPayload) -> TypedPayload:
    """TypedPayloadを送信して応答を待つ"""
    response = await channel.send_and_receive(payload.to_message())
    return TypedPayload.from_message(response)


async def send_text_and_receive_typed(channel: IDuplexChannel, text: str) -> TypedPayload:
    """テキストを送信してTypedPayloadで応答を受け取る"""
    response = await channel.send_and_receive(TypedPayload.from_text(text).to_message())
    return TypedPayload.from_message(response)


async def reply_typed(channel: IDuplexChannel, request: DuplexMessage, 
                       payload: TypedPayload) -> None:
    """TypedPayloadで応答"""
    await channel.reply(request, payload.to_message())


async def reply_typed_text(channel: IDuplexChannel, request: DuplexMessage, 
                            text: str) -> None:
    """テキストをTypedPayloadで応答"""
    await channel.reply(request, TypedPayload.from_text(text).to_message())


async def reply_typed_json(channel: IDuplexChannel, request: DuplexMessage, 
                            json_str: str) -> None:
    """JSONをTypedPayloadで応答"""
    await channel.reply(request, TypedPayload.from_json(json_str).to_message())


async def reply_typed_image(channel: IDuplexChannel, request: DuplexMessage,
                             image_data: bytes, mime_type: str = "image/png") -> None:
    """画像をTypedPayloadで応答"""
    await channel.reply(request, TypedPayload.from_image(image_data, mime_type).to_message())


# サーバー用拡張
async def broadcast_typed(server: IDuplexServer, payload: TypedPayload) -> None:
    """TypedPayloadをブロードキャスト"""
    await server.broadcast(payload.to_message())


async def broadcast_typed_text(server: IDuplexServer, text: str) -> None:
    """テキストをTypedPayloadでブロードキャスト"""
    await server.broadcast(TypedPayload.from_text(text).to_message())


async def broadcast_typed_json(server: IDuplexServer, json_str: str) -> None:
    """JSONをTypedPayloadでブロードキャスト"""
    await server.broadcast(TypedPayload.from_json(json_str).to_message())


# =============================================================================
# 名前付きパイプ双方向通信 (Windows Named Pipes / Unix Domain Socket)
# =============================================================================

def get_pipe_path(pipe_name: str) -> str:
    """
    OSに応じたパイプパスを取得
    
    - Windows: \\\\.\\pipe\\{pipe_name}
    - Unix/Linux/macOS: /tmp/{pipe_name}.sock
    
    Args:
        pipe_name: パイプ名
    
    Returns:
        フルパス
    """
    import os
    if os.name == 'nt':
        return f"\\\\.\\pipe\\{pipe_name}"
    else:
        return f"/tmp/{pipe_name}.sock"


# Windows Named Pipes用ヘルパークラス
class _WindowsPipeReader:
    """Windows Named Pipe用の非同期リーダー"""
    def __init__(self, handle, loop):
        self._handle = handle
        self._loop = loop
        self._executor = None
    
    async def read(self, n: int) -> bytes:
        """指定バイト数を読み取る"""
        import ctypes
        from ctypes import wintypes
        
        kernel32 = ctypes.windll.kernel32
        
        buffer = ctypes.create_string_buffer(n)
        bytes_read = wintypes.DWORD()
        
        def _read():
            success = kernel32.ReadFile(
                self._handle,
                buffer,
                n,
                ctypes.byref(bytes_read),
                None  # Overlapped (同期読み取り)
            )
            if not success:
                error = ctypes.get_last_error()
                if error == 109:  # ERROR_BROKEN_PIPE
                    return b''
                raise IOError(f"ReadFile failed: error {error}")
            return buffer.raw[:bytes_read.value]
        
        return await self._loop.run_in_executor(self._executor, _read)


class _WindowsPipeWriter:
    """Windows Named Pipe用の非同期ライター"""
    def __init__(self, handle, loop):
        self._handle = handle
        self._loop = loop
        self._executor = None
        self._closing = False
    
    def is_closing(self) -> bool:
        return self._closing
    
    def write(self, data: bytes) -> None:
        """データを書き込み（バッファリング）"""
        # 即座に書き込む
        import ctypes
        from ctypes import wintypes
        
        kernel32 = ctypes.windll.kernel32
        
        bytes_written = wintypes.DWORD()
        success = kernel32.WriteFile(
            self._handle,
            data,
            len(data),
            ctypes.byref(bytes_written),
            None
        )
        if not success:
            error = ctypes.get_last_error()
            raise IOError(f"WriteFile failed: error {error}")
    
    async def drain(self) -> None:
        """バッファをフラッシュ（Windows Pipeでは不要）"""
        pass
    
    def close(self) -> None:
        """パイプを閉じる"""
        import ctypes
        self._closing = True
        if self._handle:
            ctypes.windll.kernel32.CloseHandle(self._handle)
    
    async def wait_closed(self) -> None:
        """閉じるのを待つ"""
        pass


class PipeDuplexChannel(IDuplexChannel):
    r"""
    名前付きパイプ双方向通信チャネル
    
    - Windows: Windows Named Pipes (\\.\pipe\name)
    - Unix/Linux/macOS: Unix Domain Socket (/tmp/name.sock)
    """

    def __init__(self, reader: asyncio.StreamReader = None,
                 writer: asyncio.StreamWriter = None,
                 channel_id: str = None):
        import uuid
        self._reader = reader
        self._writer = writer
        self._id = channel_id or uuid.uuid4().hex[:8]
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._send_lock = asyncio.Lock()
        self._next_message_id = 0
        self._receive_task: Optional[asyncio.Task] = None
        self._closed = False
        self._pipe_handle = None  # Windows用

        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None
        self.on_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None

    @property
    def is_connected(self) -> bool:
        if self._writer is not None:
            return not self._writer.is_closing()
        return False

    @property
    def id(self) -> str:
        return self._id

    async def connect(self, pipe_name: str, timeout_ms: int = 5000) -> None:
        """サーバーに接続"""
        import os
        
        if os.name == 'nt':
            # Windows Named Pipes
            await self._connect_windows(pipe_name, timeout_ms)
        else:
            # Unix Domain Socket
            await self._connect_unix(pipe_name, timeout_ms)

    async def _connect_windows(self, pipe_name: str, timeout_ms: int) -> None:
        """Windows Named Pipesに接続"""
        import ctypes
        from ctypes import wintypes
        
        pipe_path = f"\\\\.\\pipe\\{pipe_name}"
        
        GENERIC_READ = 0x80000000
        GENERIC_WRITE = 0x40000000
        OPEN_EXISTING = 3
        INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value
        
        kernel32 = ctypes.windll.kernel32
        
        # パイプが利用可能になるまで待機
        WaitNamedPipeW = kernel32.WaitNamedPipeW
        WaitNamedPipeW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD]
        WaitNamedPipeW.restype = wintypes.BOOL
        
        if not WaitNamedPipeW(pipe_path, timeout_ms):
            raise ConnectionError(f"Pipe '{pipe_name}' not available (timeout)")
        
        # パイプを開く
        CreateFileW = kernel32.CreateFileW
        CreateFileW.argtypes = [
            wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
            ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE
        ]
        CreateFileW.restype = wintypes.HANDLE
        
        handle = CreateFileW(
            pipe_path,
            GENERIC_READ | GENERIC_WRITE,
            0,  # no sharing
            None,
            OPEN_EXISTING,
            0,  # 同期モード
            None
        )
        
        if handle == INVALID_HANDLE_VALUE:
            error = ctypes.get_last_error()
            raise ConnectionError(f"Failed to open pipe '{pipe_name}': error {error}")
        
        self._pipe_handle = handle
        
        # asyncio用のReader/Writerを作成
        loop = asyncio.get_event_loop()
        self._reader = _WindowsPipeReader(handle, loop)
        self._writer = _WindowsPipeWriter(handle, loop)
        
        self._start_receiving()

    async def _connect_unix(self, pipe_name: str, timeout_ms: int) -> None:
        """Unix Domain Socketに接続"""
        socket_path = f"/tmp/{pipe_name}.sock"

        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_unix_connection(socket_path),
                timeout=timeout_ms / 1000
            )
            self._start_receiving()
        except FileNotFoundError:
            raise ConnectionError(f"Pipe '{pipe_name}' not found at {socket_path}")

    def _start_receiving(self) -> None:
        """受信処理を開始"""
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self) -> None:
        """受信ループ"""
        try:
            while not self._closed and self._reader:
                header = await self._read_exact(DuplexPacket.HEADER_SIZE)
                if not header:
                    break

                parsed = DuplexPacket.parse_header(header)
                if not parsed:
                    break

                msg_type, message_id, payload_length, tag_length = parsed

                body_length = tag_length + payload_length
                body = b''
                if body_length > 0:
                    body = await self._read_exact(body_length)
                    if not body:
                        break

                message = DuplexPacket.parse_body(msg_type, message_id, body, tag_length)
                await self._handle_message(message)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug(f"Pipe receive loop error: {e}")
        finally:
            if not self._closed:
                await self._notify_disconnected()

            for future in self._pending_requests.values():
                if not future.done():
                    future.cancel()
            self._pending_requests.clear()

    async def _read_exact(self, count: int) -> Optional[bytes]:
        """指定バイト数を正確に読み取る"""
        data = b''
        while len(data) < count:
            # Windows Named Pipe用とUnix用で処理を分ける
            if hasattr(self._reader, 'read'):
                # 両方ともreadメソッドを持つ
                chunk = await self._reader.read(count - len(data))
            else:
                return None
            if not chunk:
                return None
            data += chunk
        return data

    async def _handle_message(self, message: DuplexMessage) -> None:
        if message.type == MessageType.RESPONSE:
            future = self._pending_requests.pop(message.id, None)
            if future and not future.done():
                future.set_result(message)
        else:
            if self.on_received:
                try:
                    result = self.on_received(self, message)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.error(f"on_received handler error: {e}")

    async def _notify_disconnected(self) -> None:
        if self.on_disconnected:
            try:
                result = self.on_disconnected(self)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_disconnected handler error: {e}")

    async def send(self, message: DuplexMessage) -> None:
        message.type = MessageType.PUSH
        self._next_message_id += 1
        message.id = self._next_message_id
        await self._send_internal(message)

    async def send_and_receive(self, message: DuplexMessage) -> DuplexMessage:
        message.type = MessageType.REQUEST
        self._next_message_id += 1
        message.id = self._next_message_id

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_requests[message.id] = future

        try:
            await self._send_internal(message)
            return await future
        finally:
            self._pending_requests.pop(message.id, None)

    async def reply(self, request: DuplexMessage, response: DuplexMessage) -> None:
        response.type = MessageType.RESPONSE
        response.id = request.id
        await self._send_internal(response)

    async def _send_internal(self, message: DuplexMessage) -> None:
        async with self._send_lock:
            packet = DuplexPacket.serialize(message)
            self._writer.write(packet)
            await self._writer.drain()

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True

        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        if self._writer:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass

        # Windows Named Pipe用のハンドルをクローズ
        if self._pipe_handle:
            import os
            if os.name == 'nt':
                import ctypes
                ctypes.windll.kernel32.CloseHandle(self._pipe_handle)
            self._pipe_handle = None

        await self._notify_disconnected()


class PipeDuplexClient(IDuplexChannel):
    """名前付きパイプ双方向通信クライアント"""

    def __init__(self, pipe_name: str):
        self._pipe_name = pipe_name
        self._channel: Optional[PipeDuplexChannel] = None

        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None
        self.on_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None

    @property
    def is_connected(self) -> bool:
        return self._channel.is_connected if self._channel else False

    @property
    def id(self) -> str:
        return self._channel.id if self._channel else ""

    async def connect(self, timeout_ms: int = 5000) -> None:
        if self._channel:
            raise RuntimeError("Already connected.")

        self._channel = PipeDuplexChannel()
        self._channel.on_received = self._on_received
        self._channel.on_disconnected = self._on_disconnected
        await self._channel.connect(self._pipe_name, timeout_ms)

    async def reconnect(self, timeout_ms: int = 5000) -> None:
        if self._channel:
            await self._channel.close()
            self._channel = None
        await self.connect(timeout_ms)

    async def _on_received(self, ch: IDuplexChannel, msg: DuplexMessage) -> None:
        if self.on_received:
            await self.on_received(self, msg)

    async def _on_disconnected(self, ch: IDuplexChannel) -> None:
        if self.on_disconnected:
            await self.on_disconnected(self)

    async def send(self, message: DuplexMessage) -> None:
        self._ensure_connected()
        await self._channel.send(message)

    async def send_and_receive(self, message: DuplexMessage) -> DuplexMessage:
        self._ensure_connected()
        return await self._channel.send_and_receive(message)

    async def reply(self, request: DuplexMessage, response: DuplexMessage) -> None:
        self._ensure_connected()
        await self._channel.reply(request, response)

    async def close(self) -> None:
        if self._channel:
            await self._channel.close()

    def _ensure_connected(self) -> None:
        if not self._channel or not self._channel.is_connected:
            raise RuntimeError("Not connected.")


class PipeDuplexServer(IDuplexServer):
    """名前付きパイプ双方向通信サーバー"""

    def __init__(self, pipe_name: str, max_clients: int = 16):
        self._pipe_name = pipe_name
        self._max_clients = max_clients
        self._server: Optional[asyncio.Server] = None
        self._clients: Dict[str, PipeDuplexChannel] = {}
        self._client_id_counter = 0
        self._socket_path: str = ""

        self.on_client_connected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
        self.on_client_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None

    @property
    def is_listening(self) -> bool:
        return self._server is not None and self._server.is_serving()

    @property
    def clients(self) -> List[IDuplexChannel]:
        return list(self._clients.values())

    async def start(self, port: int = 0) -> None:
        """リッスン開始（portは無視、pipe_nameで識別）"""
        import os

        if os.name == 'nt':
            raise NotImplementedError("Windows named pipes are not supported in asyncio. Use TCP instead.")

        self._socket_path = f"/tmp/{self._pipe_name}.sock"

        # 既存のソケットファイルを削除
        if os.path.exists(self._socket_path):
            os.unlink(self._socket_path)

        self._server = await asyncio.start_unix_server(
            self._handle_connection,
            self._socket_path
        )

    async def _handle_connection(self, reader: asyncio.StreamReader,
                                  writer: asyncio.StreamWriter) -> None:
        self._client_id_counter += 1
        client_id = f"P{self._client_id_counter:04d}"

        channel = PipeDuplexChannel(reader, writer, client_id)
        channel.on_received = self._on_channel_received
        channel.on_disconnected = self._on_channel_disconnected

        self._clients[client_id] = channel
        channel._start_receiving()

        if self.on_client_connected:
            try:
                result = self.on_client_connected(channel)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_client_connected handler error: {e}")

    async def _on_channel_received(self, ch: IDuplexChannel, msg: DuplexMessage) -> None:
        if self.on_received:
            await self.on_received(ch, msg)

    async def _on_channel_disconnected(self, ch: IDuplexChannel) -> None:
        self._clients.pop(ch.id, None)
        if self.on_client_disconnected:
            try:
                result = self.on_client_disconnected(ch)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_client_disconnected handler error: {e}")

    async def broadcast(self, message: DuplexMessage) -> None:
        tasks = [client.send(message) for client in self._clients.values()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def broadcast_except(self, exclude_client_id: str, message: DuplexMessage) -> None:
        tasks = [
            client.send(message)
            for client in self._clients.values()
            if client.id != exclude_client_id
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        import os

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        for client in list(self._clients.values()):
            await client.close()
        self._clients.clear()

        # ソケットファイルを削除
        if self._socket_path and os.path.exists(self._socket_path):
            os.unlink(self._socket_path)


# =============================================================================
# 使用例
# =============================================================================

if __name__ == "__main__":
    async def example():
        """基本的な使用例"""
        port = 12345

        # サーバー
        server = TcpDuplexServer()

        async def on_received(client: IDuplexChannel, msg: DuplexMessage):
            payload = msg.to_typed_payload()
            print(f"[Server] Received {payload.count} items:")
            for item in payload:
                print(f"  - {item}")

            # 応答
            response = TypedPayload().add_text("OK").add_json('{"status":"ok"}')
            await reply_typed(client, msg, response)

        server.on_received = on_received
        await server.start(port)
        print(f"[Server] Listening on port {port}")

        # クライアント
        client = TcpDuplexClient("localhost", port)
        await client.connect()
        print("[Client] Connected")

        # 複数アイテムを1パケットで送信
        packet = (TypedPayload()
            .add_text("タイトル: テスト画像")
            .add_json('{"width":640,"height":480}')
            .add_image(bytes([0x89, 0x50, 0x4E, 0x47]), "image/png")
            .add_binary(bytes([1, 2, 3, 4, 5]), "application/x-mesh"))

        print(f"[Client] Sending {packet.count} items...")
        response = await send_and_receive_typed(client, packet)

        print(f"[Client] Response: {response.count} items")
        print(f"  Text: {response.get_text()}")
        print(f"  JSON: {response.get_json()}")

        await client.close()
        await server.stop()

    asyncio.run(example())


# =============================================================================
# WebSocket双方向通信 (ブラウザ対応)
# =============================================================================

class WebSocketDuplexChannel(IDuplexChannel):
    """
    WebSocket双方向通信チャネル
    JSON形式でメッセージを送受信（ブラウザ互換）
    """

    def __init__(self, websocket=None, channel_id: str = None):
        import uuid
        self._websocket = websocket
        self._id = channel_id or uuid.uuid4().hex[:8]
        self._pending_requests: Dict[int, asyncio.Future] = {}
        self._send_lock = asyncio.Lock()
        self._next_message_id = 0
        self._receive_task: Optional[asyncio.Task] = None
        self._closed = False

        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None
        self.on_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None

    @property
    def is_connected(self) -> bool:
        return self._websocket is not None and not self._closed

    @property
    def id(self) -> str:
        return self._id

    def _start_receiving(self) -> None:
        """受信処理を開始"""
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def _receive_loop(self) -> None:
        """受信ループ"""
        try:
            async for raw_message in self._websocket:
                if isinstance(raw_message, str):
                    await self._handle_json_message(raw_message)
                elif isinstance(raw_message, bytes):
                    await self._handle_binary_message(raw_message)
        except Exception as e:
            logger.debug(f"WebSocket receive error: {e}")
        finally:
            if not self._closed:
                await self._notify_disconnected()

            for future in self._pending_requests.values():
                if not future.done():
                    future.cancel()
            self._pending_requests.clear()

    async def _handle_json_message(self, json_str: str) -> None:
        """JSONメッセージを処理"""
        import json
        try:
            data = json.loads(json_str)
            msg_type_str = data.get("type", "push")
            msg_id = data.get("id", 0)

            # TypedPayloadを構築
            if "items" in data and isinstance(data["items"], list):
                payload = self._parse_items_from_json(data["items"])
            elif "text" in data:
                payload = TypedPayload.from_text(data["text"])
            elif "json" in data:
                json_data = data["json"]
                if isinstance(json_data, str):
                    payload = TypedPayload.from_json(json_data)
                else:
                    payload = TypedPayload.from_json(json.dumps(json_data))
            else:
                payload = TypedPayload()

            msg_type = {
                "push": MessageType.PUSH,
                "request": MessageType.REQUEST,
                "response": MessageType.RESPONSE
            }.get(msg_type_str, MessageType.PUSH)

            message = DuplexMessage(
                id=msg_id,
                type=msg_type,
                payload=payload.serialize()
            )

            await self._handle_message(message)

        except Exception as e:
            logger.debug(f"JSON parse error: {e}")

    def _parse_items_from_json(self, items: list) -> TypedPayload:
        """JSONアイテム配列をTypedPayloadに変換"""
        import base64
        payload = TypedPayload()

        for item in items:
            item_type = item.get("type", 0)
            mime_type = item.get("mimeType", "")
            data = item.get("data", "")
            encoding = item.get("encoding", "")

            if encoding == "base64":
                data_bytes = base64.b64decode(data)
            else:
                data_bytes = data.encode("utf-8") if isinstance(data, str) else data

            content_type = ContentType(item_type)
            if content_type == ContentType.TEXT:
                payload.add_text(data if isinstance(data, str) else data_bytes.decode("utf-8"))
            elif content_type == ContentType.JSON:
                payload.add_json(data if isinstance(data, str) else data_bytes.decode("utf-8"))
            elif content_type == ContentType.IMAGE:
                payload.add_image(data_bytes, mime_type or "image/png")
            elif content_type == ContentType.BINARY:
                payload.add_binary(data_bytes, mime_type or "application/octet-stream")
            else:
                payload.add_custom(data_bytes, mime_type or "application/octet-stream")

        return payload

    async def _handle_binary_message(self, data: bytes) -> None:
        """バイナリメッセージを処理（DuplexPacketフォーマット）"""
        if len(data) < DuplexPacket.HEADER_SIZE:
            return

        parsed = DuplexPacket.parse_header(data[:DuplexPacket.HEADER_SIZE])
        if not parsed:
            return

        msg_type, message_id, payload_length, tag_length = parsed
        body = data[DuplexPacket.HEADER_SIZE:]
        message = DuplexPacket.parse_body(msg_type, message_id, body, tag_length)
        await self._handle_message(message)

    async def _handle_message(self, message: DuplexMessage) -> None:
        """受信メッセージを処理"""
        if message.type == MessageType.RESPONSE:
            future = self._pending_requests.pop(message.id, None)
            if future and not future.done():
                future.set_result(message)
        else:
            if self.on_received:
                try:
                    result = self.on_received(self, message)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.error(f"on_received handler error: {e}")

    async def _notify_disconnected(self) -> None:
        if self.on_disconnected:
            try:
                result = self.on_disconnected(self)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_disconnected handler error: {e}")

    async def send(self, message: DuplexMessage) -> None:
        message.type = MessageType.PUSH
        self._next_message_id += 1
        message.id = self._next_message_id
        await self._send_internal(message)

    async def send_and_receive(self, message: DuplexMessage) -> DuplexMessage:
        message.type = MessageType.REQUEST
        self._next_message_id += 1
        message.id = self._next_message_id

        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self._pending_requests[message.id] = future

        try:
            await self._send_internal(message)
            return await future
        except:
            self._pending_requests.pop(message.id, None)
            raise

    async def reply(self, request: DuplexMessage, response: DuplexMessage) -> None:
        response.type = MessageType.RESPONSE
        response.id = request.id
        await self._send_internal(response)

    async def _send_internal(self, message: DuplexMessage) -> None:
        """JSON形式で送信"""
        import json
        import base64

        async with self._send_lock:
            payload = message.to_typed_payload()

            type_str = {
                MessageType.PUSH: "push",
                MessageType.REQUEST: "request",
                MessageType.RESPONSE: "response"
            }.get(message.type, "push")

            items = []
            for item in payload:
                item_dict = {
                    "type": item.type.value,
                    "mimeType": item.mime_type
                }

                if item.type in (ContentType.TEXT, ContentType.JSON):
                    item_dict["data"] = item.data_string or ""
                else:
                    item_dict["data"] = base64.b64encode(item.data or b"").decode("ascii")
                    item_dict["encoding"] = "base64"

                items.append(item_dict)

            json_data = {
                "type": type_str,
                "id": message.id,
                "items": items
            }

            await self._websocket.send(json.dumps(json_data))

    async def close(self) -> None:
        if self._closed:
            return

        self._closed = True

        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass

        if self._websocket:
            try:
                await self._websocket.close()
            except Exception:
                pass

        await self._notify_disconnected()


class WebSocketDuplexClient(IDuplexChannel):
    """
    WebSocket双方向通信クライアント
    
    使用例:
        client = WebSocketDuplexClient("ws://localhost:8080/")
        await client.connect()
        response = await client.send_and_receive_text("Hello!")
        print(response.payload_string)
        await client.close()
    """

    def __init__(self, uri: str):
        self._uri = uri
        self._channel: Optional[WebSocketDuplexChannel] = None
        self._websocket = None

        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None
        self.on_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None

    @property
    def is_connected(self) -> bool:
        return self._channel.is_connected if self._channel else False

    @property
    def id(self) -> str:
        return self._channel.id if self._channel else ""

    async def connect(self) -> None:
        """サーバーに接続"""
        try:
            import websockets
        except ImportError:
            raise ImportError("websockets is required. Install with: pip install websockets")

        if self._channel:
            raise RuntimeError("Already connected.")

        self._websocket = await websockets.connect(self._uri)
        self._channel = WebSocketDuplexChannel(self._websocket)
        self._channel.on_received = self._on_received
        self._channel.on_disconnected = self._on_disconnected
        self._channel._start_receiving()

    async def reconnect(self) -> None:
        """再接続"""
        if self._channel:
            await self._channel.close()
            self._channel = None
        await self.connect()

    async def _on_received(self, ch: IDuplexChannel, msg: DuplexMessage) -> None:
        if self.on_received:
            await self.on_received(self, msg)

    async def _on_disconnected(self, ch: IDuplexChannel) -> None:
        if self.on_disconnected:
            await self.on_disconnected(self)

    async def send(self, message: DuplexMessage) -> None:
        self._ensure_connected()
        await self._channel.send(message)

    async def send_text(self, text: str) -> None:
        await self.send(DuplexMessage.from_text(text))

    async def send_and_receive(self, message: DuplexMessage) -> DuplexMessage:
        self._ensure_connected()
        return await self._channel.send_and_receive(message)

    async def send_and_receive_text(self, text: str) -> DuplexMessage:
        return await self.send_and_receive(DuplexMessage.from_text(text))

    async def reply(self, request: DuplexMessage, response: DuplexMessage) -> None:
        self._ensure_connected()
        await self._channel.reply(request, response)

    async def close(self) -> None:
        if self._channel:
            await self._channel.close()

    def _ensure_connected(self) -> None:
        if not self._channel or not self._channel.is_connected:
            raise RuntimeError("Not connected.")


class WebSocketDuplexServer(IDuplexServer):
    """
    WebSocket双方向通信サーバー
    ブラウザから直接接続可能
    
    使用例:
        server = WebSocketDuplexServer()
        server.on_received = lambda ch, msg: print(msg.to_typed_payload().get_text())
        await server.start(8080)
    """

    def __init__(self):
        self._server = None
        self._clients: Dict[str, WebSocketDuplexChannel] = {}
        self._client_id_counter = 0

        self.on_client_connected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
        self.on_client_disconnected: Optional[Callable[[IDuplexChannel], Awaitable[None]]] = None
        self.on_received: Optional[Callable[[IDuplexChannel, DuplexMessage], Awaitable[None]]] = None

    @property
    def is_listening(self) -> bool:
        return self._server is not None

    @property
    def clients(self) -> List[IDuplexChannel]:
        return list(self._clients.values())

    async def start(self, port: int, host: str = "0.0.0.0") -> None:
        """リッスン開始"""
        try:
            import websockets
        except ImportError:
            raise ImportError("websockets is required. Install with: pip install websockets")

        if self._server:
            raise RuntimeError("Server is already running.")

        self._server = await websockets.serve(
            self._handle_connection,
            host,
            port
        )

    async def _handle_connection(self, websocket, path=None) -> None:
        """新規接続を処理"""
        self._client_id_counter += 1
        client_id = f"W{self._client_id_counter:04d}"

        channel = WebSocketDuplexChannel(websocket, client_id)
        channel.on_received = self._on_channel_received
        channel.on_disconnected = self._on_channel_disconnected

        self._clients[client_id] = channel

        if self.on_client_connected:
            try:
                result = self.on_client_connected(channel)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_client_connected handler error: {e}")

        # 受信ループ（接続が切れるまでブロック）
        try:
            async for raw_message in websocket:
                if isinstance(raw_message, str):
                    await channel._handle_json_message(raw_message)
                elif isinstance(raw_message, bytes):
                    await channel._handle_binary_message(raw_message)
        except Exception as e:
            logger.debug(f"WebSocket connection error: {e}")
        finally:
            self._clients.pop(client_id, None)
            if channel.on_disconnected:
                try:
                    result = channel.on_disconnected(channel)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.error(f"on_disconnected handler error: {e}")

    async def _on_channel_received(self, ch: IDuplexChannel, msg: DuplexMessage) -> None:
        if self.on_received:
            await self.on_received(ch, msg)

    async def _on_channel_disconnected(self, ch: IDuplexChannel) -> None:
        self._clients.pop(ch.id, None)
        if self.on_client_disconnected:
            try:
                result = self.on_client_disconnected(ch)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_client_disconnected handler error: {e}")

    async def broadcast(self, message: DuplexMessage) -> None:
        """全クライアントにブロードキャスト"""
        tasks = [client.send(message) for client in self._clients.values()]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def broadcast_text(self, text: str) -> None:
        await self.broadcast(DuplexMessage.from_text(text))

    async def broadcast_except(self, exclude_client_id: str, message: DuplexMessage) -> None:
        """特定クライアント以外に送信"""
        tasks = [
            client.send(message)
            for client in self._clients.values()
            if client.id != exclude_client_id
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """サーバー停止"""
        for client in list(self._clients.values()):
            try:
                await client.close()
            except Exception:
                pass
        self._clients.clear()

        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None


# WebSocket用ヘルパー関数
async def ws_send_typed_payload(channel: WebSocketDuplexChannel, payload: TypedPayload) -> None:
    """TypedPayloadをWebSocket経由で送信"""
    await channel.send(payload.to_message())


async def ws_send_and_receive_typed(channel: WebSocketDuplexChannel, payload: TypedPayload) -> TypedPayload:
    """TypedPayloadを送信して応答を待つ"""
    response = await channel.send_and_receive(payload.to_message())
    return TypedPayload.from_message(response)


async def ws_reply_typed(channel: WebSocketDuplexChannel, request: DuplexMessage, payload: TypedPayload) -> None:
    """TypedPayloadで応答"""
    await channel.reply(request, payload.to_message())
