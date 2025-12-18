"""
HagLib Duplex 使用例 (Python版)

Usage:
    python haglib_duplex_example.py
"""

import asyncio
from haglib_duplex import (
    TcpDuplexServer, TcpDuplexClient,
    UdpDuplexChannel,
    PipeDuplexServer, PipeDuplexClient,
    DuplexMessage, IDuplexChannel,
    TypedPayload, TypedPayloadItem, ContentType,
    send_and_receive_typed, reply_typed, reply_typed_text
)


async def mixed_payload_example():
    """複数アイテム混在の例"""
    print("=== Mixed Payload Example ===\n")
    
    port = 12370

    server = TcpDuplexServer()

    async def on_received(client: IDuplexChannel, msg: DuplexMessage):
        payload = msg.to_typed_payload()
        
        print(f"[Server] Received {payload.count} items:")
        for item in payload:
            print(f"  - {item}")

        # 各型ごとに処理
        text = payload.get_text()
        if text:
            print(f"  Text: {text}")

        json_str = payload.get_json()
        if json_str:
            print(f"  JSON: {json_str}")

        image = payload.get_image()
        if image:
            print(f"  Image: {len(image)} bytes ({payload.get_image_mime_type()})")

        # 応答も複数アイテムで返す
        response = (TypedPayload()
            .add_text("Received!")
            .add_json(f'{{"status":"ok","count":{payload.count}}}'))

        await reply_typed(client, msg, response)

    server.on_received = on_received
    await server.start(port)
    print(f"[Server] Listening on port {port}")

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
    print()


def chain_example():
    """チェーン構文の例"""
    print("=== Chain Example ===\n")

    # メソッドチェーンで構築
    payload = (TypedPayload()
        .add_text("Hello")
        .add_text("World")
        .add_json('{"key":"value"}')
        .add_image(bytes([0xFF, 0xD8, 0xFF]), "image/jpeg")
        .add_custom(bytes([1, 2, 3]), "application/x-fpx"))

    print(f"Built payload with {payload.count} items")

    # foreach で列挙可能
    for item in payload:
        print(f"  {item.type.name}: {item.mime_type}")

    # インデクサでアクセス
    print(f"First item: {payload[0]}")
    print()


async def multiple_images_example():
    """画像複数枚送信の例"""
    print("=== Multiple Images Example ===\n")

    port = 12371

    server = TcpDuplexServer()

    async def on_received(client: IDuplexChannel, msg: DuplexMessage):
        payload = msg.to_typed_payload()

        print("[Server] Received images:")
        index = 0
        for data, mime in payload.get_all_images():
            print(f"  Image {index}: {mime}, {len(data)} bytes")
            index += 1

        await reply_typed_text(client, msg, f"Received {index} images")

    server.on_received = on_received
    await server.start(port)

    client = TcpDuplexClient("localhost", port)
    await client.connect()

    # 複数画像を送信
    packet = (TypedPayload()
        .add_image(bytes([0x89, 0x50, 0x4E, 0x47, 0x00, 0x01]), "image/png")
        .add_image(bytes([0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10]), "image/jpeg")
        .add_image(bytes([0x47, 0x49, 0x46, 0x38, 0x39, 0x61]), "image/gif"))

    response = await send_and_receive_typed(client, packet)
    print(f"[Client] {response.get_text()}")

    await client.close()
    await server.stop()
    print()


async def single_item_example():
    """単一アイテム送信（従来互換）"""
    print("=== Single Item Example ===\n")

    port = 12372

    server = TcpDuplexServer()

    async def on_received(client: IDuplexChannel, msg: DuplexMessage):
        payload = msg.to_typed_payload()

        # 単一アイテムでも Count=1 のリストとして扱う
        if payload.count == 1:
            item = payload[0]
            print(f"[Server] Single item: {item}")

            if item.type == ContentType.TEXT:
                await reply_typed_text(client, msg, f"Echo: {item.data_string}")

    server.on_received = on_received
    await server.start(port)

    client = TcpDuplexClient("localhost", port)
    await client.connect()

    # ファクトリメソッドで単一送信（内部では Count=1 のリスト）
    from haglib_duplex import send_text_and_receive_typed
    response = await send_text_and_receive_typed(client, "Hello!")
    print(f"[Client] {response.get_text()}")

    # ファクトリメソッドでも同じ
    payload = TypedPayload.from_json('{"action":"test"}')
    print(f"FromJson creates {payload.count} item(s)")

    await client.close()
    await server.stop()
    print()


async def fpx_with_metadata_example():
    """FPXデータ＋メタ情報の例"""
    print("=== FPX with Metadata Example ===\n")

    port = 12373
    FPX_MIME = "application/x-fpx"

    server = TcpDuplexServer()

    async def on_received(client: IDuplexChannel, msg: DuplexMessage):
        payload = msg.to_typed_payload()

        # メタ情報（JSON）を取得
        meta = payload.get_json()
        print(f"[Server] Metadata: {meta}")

        # FPXデータを取得
        fpx_item = payload.get_first_by_mime(FPX_MIME)
        if fpx_item:
            print(f"[Server] FPX data: {len(fpx_item.data)} bytes")
            # FPXデータを処理...

        # サムネイル画像があれば取得
        thumbnail = payload.get_image()
        if thumbnail:
            print(f"[Server] Thumbnail: {len(thumbnail)} bytes")

        await reply_typed_text(client, msg, "FPX processed")

    server.on_received = on_received
    await server.start(port)

    client = TcpDuplexClient("localhost", port)
    await client.connect()

    # FPXデータ＋メタ情報＋サムネイルを1パケットで送信
    packet = (TypedPayload()
        .add_json('{"name":"MyModel","version":1}')
        .add_custom(bytes([0x01, 0x02, 0x03]), FPX_MIME)  # FPXバイナリ
        .add_image(bytes([0x89, 0x50, 0x4E, 0x47]), "image/png"))  # PNGサムネイル

    response = await send_and_receive_typed(client, packet)
    print(f"[Client] {response.get_text()}")

    await client.close()
    await server.stop()
    print()


def serialization_test():
    """シリアライズ/デシリアライズのテスト"""
    print("=== Serialization Test ===\n")

    original = (TypedPayload()
        .add_text("Hello")
        .add_json('{"test":true}')
        .add_image(bytes([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A]), "image/png")
        .add_binary(bytes([0xFF, 0xFE, 0xFD]))
        .add_custom(bytes([1, 2, 3]), "application/x-custom"))

    print(f"Original: {original.count} items")

    # シリアライズ
    data = original.serialize()
    print(f"Serialized: {len(data)} bytes")

    # デシリアライズ
    restored = TypedPayload.deserialize(data)
    print(f"Restored: {restored.count} items")

    # 検証
    for i in range(original.count):
        o = original[i]
        r = restored[i]
        match = o.type == r.type and o.mime_type == r.mime_type
        print(f"  [{i}] {o.type.name}: {'OK' if match else 'MISMATCH'}")

    print("\n=== Test Complete ===\n")


async def udp_example():
    """UDP通信の例"""
    print("=== UDP Example ===\n")

    port = 12374

    # サーバー側
    server = UdpDuplexChannel()
    await server.bind(port)

    async def on_server_received(ch: UdpDuplexChannel, msg: DuplexMessage, addr):
        print(f"[UDP Server] Received from {addr}: {msg.payload_string}")
        await ch.reply_text(msg, "UDP Reply!", addr)

    server.on_received = on_server_received
    print(f"[UDP Server] Listening on port {port}")

    # クライアント側
    client = UdpDuplexChannel()
    await client.connect("localhost", port)
    print("[UDP Client] Connected")

    # リクエスト/レスポンス
    response = await client.send_and_receive(DuplexMessage.from_text("Hello UDP!"), timeout_ms=3000)
    print(f"[UDP Client] Response: {response.payload_string}")

    client.close()
    server.close()
    print()


async def simple_tcp_example():
    """シンプルなTCP通信の例"""
    print("=== Simple TCP Example ===\n")

    port = 12375

    # サーバー
    server = TcpDuplexServer()

    async def on_received(client: IDuplexChannel, msg: DuplexMessage):
        print(f"[Server] Received: {msg.payload_string}")
        await client.reply_text(msg, f"Echo: {msg.payload_string}")

    async def on_connected(client: IDuplexChannel):
        print(f"[Server] Client connected: {client.id}")

    async def on_disconnected(client: IDuplexChannel):
        print(f"[Server] Client disconnected: {client.id}")

    server.on_received = on_received
    server.on_client_connected = on_connected
    server.on_client_disconnected = on_disconnected

    await server.start(port)
    print(f"[Server] Listening on port {port}")

    # クライアント
    client = TcpDuplexClient("localhost", port)
    await client.connect()
    print("[Client] Connected")

    # プッシュ送信
    await client.send_text("Push message (no response expected)")
    await asyncio.sleep(0.1)  # サーバーがログを出すのを待つ

    # リクエスト/レスポンス
    response = await client.send_and_receive_text("Request message")
    print(f"[Client] Response: {response.payload_string}")

    await client.close()
    await server.stop()
    print()


async def main():
    """全ての例を実行"""
    await simple_tcp_example()
    chain_example()
    serialization_test()
    await mixed_payload_example()
    await multiple_images_example()
    await single_item_example()
    await fpx_with_metadata_example()
    await udp_example()


if __name__ == "__main__":
    asyncio.run(main())
