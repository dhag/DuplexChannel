using System;
using System.Diagnostics;
using System.Threading.Tasks;
using HagLib.NET.Duplex;

namespace HagLib.NET.Duplex.Examples
{
    /// <summary>
    /// TypedPayload の使用例
    /// </summary>
    public static class TypedPayloadExample
    {
        /// <summary>
        /// 複数アイテム混在の例
        /// </summary>
        public static async Task MixedPayloadExample()
        {
            const int port = 12370;

            using var server = new TcpDuplexServer();

            server.OnReceived += async (client, msg) =>
            {
                var payload = msg.ToTypedPayload();

                Debug.WriteLine($"[Server] Received {payload.Count} items:");

                foreach (var item in payload)
                {
                    Debug.WriteLine($"  - {item}");
                }

                // 各型ごとに処理
                var text = payload.GetText();
                if (text != null)
                    Debug.WriteLine($"  Text: {text}");

                var json = payload.GetJson();
                if (json != null)
                    Debug.WriteLine($"  JSON: {json}");

                var image = payload.GetImage();
                if (image != null)
                    Debug.WriteLine($"  Image: {image.Length} bytes ({payload.GetImageMimeType()})");

                // 応答も複数アイテムで返す
                var response = new TypedPayload()
                    .AddText("Received!")
                    .AddJson("{\"status\":\"ok\",\"count\":" + payload.Count + "}");

                await client.ReplyAsync(msg, response);
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();

            // 複数アイテムを1パケットで送信
            var packet = new TypedPayload()
                .AddText("タイトル: テスト画像")
                .AddJson("{\"width\":640,\"height\":480}")
                .AddImage(new byte[] { 0x89, 0x50, 0x4E, 0x47 }, "image/png")
                .AddBinary(new byte[] { 1, 2, 3, 4, 5 }, "application/x-mesh");

            Debug.WriteLine($"[Client] Sending {packet.Count} items...");

            var response = await client.SendAndReceiveAsync(packet);

            Debug.WriteLine($"[Client] Response: {response.Count} items");
            Debug.WriteLine($"  Text: {response.GetText()}");
            Debug.WriteLine($"  JSON: {response.GetJson()}");

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// チェーン構文の例
        /// </summary>
        public static void ChainExample()
        {
            // メソッドチェーンで構築
            var payload = new TypedPayload()
                .AddText("Hello")
                .AddText("World")
                .AddJson("{\"key\":\"value\"}")
                .AddImage(new byte[] { 0xFF, 0xD8, 0xFF }, "image/jpeg")
                .AddCustom(new byte[] { 1, 2, 3 }, "application/x-fpx");

            Debug.WriteLine($"Built payload with {payload.Count} items");

            // foreach で列挙可能
            foreach (var item in payload)
            {
                Debug.WriteLine($"  {item.Type}: {item.MimeType}");
            }

            // インデクサでアクセス
            Debug.WriteLine($"First item: {payload[0]}");
        }

        /// <summary>
        /// 画像複数枚送信の例
        /// </summary>
        public static async Task MultipleImagesExample()
        {
            const int port = 12371;

            using var server = new TcpDuplexServer();

            server.OnReceived += async (client, msg) =>
            {
                var payload = msg.ToTypedPayload();

                Debug.WriteLine("[Server] Received images:");

                int index = 0;
                foreach (var (data, mime) in payload.GetAllImages())
                {
                    Debug.WriteLine($"  Image {index++}: {mime}, {data.Length} bytes");
                }

                await client.ReplyTextAsync(msg, $"Received {index} images");
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();

            // 複数画像を送信
            var packet = new TypedPayload()
                .AddImage(new byte[] { 0x89, 0x50, 0x4E, 0x47, 0x00, 0x01 }, "image/png")
                .AddImage(new byte[] { 0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10 }, "image/jpeg")
                .AddImage(new byte[] { 0x47, 0x49, 0x46, 0x38, 0x39, 0x61 }, "image/gif");

            var response = await client.SendAndReceiveAsync(packet);
            Debug.WriteLine($"[Client] {response.GetText()}");

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// 単一アイテム送信（従来互換）
        /// </summary>
        public static async Task SingleItemExample()
        {
            const int port = 12372;

            using var server = new TcpDuplexServer();

            server.OnReceived += async (client, msg) =>
            {
                var payload = msg.ToTypedPayload();

                // 単一アイテムでも Count=1 のリストとして扱う
                if (payload.Count == 1)
                {
                    var item = payload[0];
                    Debug.WriteLine($"[Server] Single item: {item}");

                    if (item.Type == ContentType.Text)
                    {
                        await client.ReplyTextAsync(msg, $"Echo: {item.DataString}");
                    }
                }
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();

            // 拡張メソッドで単一送信（内部では Count=1 のリスト）
            var response = await client.SendTextAndReceiveAsync("Hello!");
            Debug.WriteLine($"[Client] {response.GetText()}");

            // ファクトリメソッドでも同じ
            var payload = TypedPayload.FromJson("{\"action\":\"test\"}");
            Debug.WriteLine($"FromJson creates {payload.Count} item(s)");

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// FPXデータ＋メタ情報の例
        /// </summary>
        public static async Task FpxWithMetadataExample()
        {
            const int port = 12373;
            const string FpxMime = "application/x-fpx";

            using var server = new TcpDuplexServer();

            server.OnReceived += async (client, msg) =>
            {
                var payload = msg.ToTypedPayload();

                // メタ情報（JSON）を取得
                var meta = payload.GetJson();
                Debug.WriteLine($"[Server] Metadata: {meta}");

                // FPXデータを取得
                var fpxItem = payload.GetFirstByMime(FpxMime);
                if (fpxItem != null)
                {
                    Debug.WriteLine($"[Server] FPX data: {fpxItem.Data.Length} bytes");
                    // FPXデータを処理...
                }

                // サムネイル画像があれば取得
                var thumbnail = payload.GetImage();
                if (thumbnail != null)
                {
                    Debug.WriteLine($"[Server] Thumbnail: {thumbnail.Length} bytes");
                }

                await client.ReplyTextAsync(msg, "FPX processed");
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();

            // FPXデータ＋メタ情報＋サムネイルを1パケットで送信
            var packet = new TypedPayload()
                .AddJson("{\"name\":\"MyModel\",\"version\":1}")
                .AddCustom(new byte[] { /* FPXバイナリ */ 0x01, 0x02, 0x03 }, FpxMime)
                .AddImage(new byte[] { /* PNGサムネイル */ 0x89, 0x50, 0x4E, 0x47 }, "image/png");

            var response = await client.SendAndReceiveAsync(packet);
            Debug.WriteLine($"[Client] {response.GetText()}");

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// シリアライズ/デシリアライズのテスト
        /// </summary>
        public static void SerializationTest()
        {
            Debug.WriteLine("=== Serialization Test ===\n");

            var original = new TypedPayload()
                .AddText("Hello")
                .AddJson("{\"test\":true}")
                .AddImage(new byte[] { 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A }, "image/png")
                .AddBinary(new byte[] { 0xFF, 0xFE, 0xFD })
                .AddCustom(new byte[] { 1, 2, 3 }, "application/x-custom");

            Debug.WriteLine($"Original: {original.Count} items");

            // シリアライズ
            var bytes = original.Serialize();
            Debug.WriteLine($"Serialized: {bytes.Length} bytes");

            // デシリアライズ
            var restored = TypedPayload.Deserialize(bytes);
            Debug.WriteLine($"Restored: {restored.Count} items");

            // 検証
            for (int i = 0; i < original.Count; i++)
            {
                var o = original[i];
                var r = restored[i];
                var match = o.Type == r.Type && o.MimeType == r.MimeType;
                Debug.WriteLine($"  [{i}] {o.Type}: {(match ? "OK" : "MISMATCH")}");
            }

            Debug.WriteLine("\n=== Test Complete ===");
        }
    }
}