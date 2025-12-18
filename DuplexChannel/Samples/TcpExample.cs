using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using HagLib.NET.Duplex;

namespace HagLib.NET.Duplex.Examples
{
    /// <summary>
    /// TCP双方向通信の使用例
    /// </summary>
    public static class TcpExample
    {
        /// <summary>
        /// 基本的な双方向通信の例
        /// </summary>
        public static async Task BasicExample()
        {
            const int port = 12345;

            // ========== サーバー側 ==========
            using var server = new TcpDuplexServer();

            server.OnClientConnected += (client) =>
            {
                Debug.WriteLine($"[Server] Client connected: {client.Id}");
            };

            server.OnClientDisconnected += (client) =>
            {
                Debug.WriteLine($"[Server] Client disconnected: {client.Id}");
            };

            server.OnReceived += async (client, msg) =>
            {
                Debug.WriteLine($"[Server] Received from {client.Id}: {msg.PayloadString}");

                if (msg.Type == MessageType.Request)
                {
                    await client.ReplyAsync(msg, $"Response to: {msg.PayloadString}");
                }
            };

            await server.StartAsync(port);
            Debug.WriteLine($"[Server] Listening on port: {port}");

            await Task.Delay(100);  // ← サーバー起動待ち追加

            // ========== クライアント側 ==========
            using var client = new TcpDuplexClient("localhost", port);

            client.OnReceived += (ch, msg) =>
            {
                Debug.WriteLine($"[Client] Received: {msg.PayloadString}");
            };

            client.OnDisconnected += (ch) =>
            {
                Debug.WriteLine("[Client] Disconnected");
            };

            await client.ConnectAsync();
            Debug.WriteLine("[Client] Connected");

            // ========== 通信 ==========
            // プッシュ送信
            await client.SendAsync("Hello from client!");

            // リクエスト/レスポンス
            var response = await client.SendAndReceiveAsync("Ping");
            Debug.WriteLine($"[Client] Response: {response.PayloadString}");

            // サーバーからブロードキャスト
            await server.BroadcastAsync("Broadcast from server!");

            await Task.Delay(100);

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// 複数クライアントの例
        /// </summary>
        public static async Task MultiClientExample()
        {
            const int port = 12346;

            using var server = new TcpDuplexServer();

            server.OnClientConnected += (client) =>
            {
                Debug.WriteLine($"[Server] Client {client.Id} connected. Total: {server.Clients.Length}");
            };

            server.OnReceived += async (sender, msg) =>
            {
                // 他のクライアントに転送
                await server.BroadcastExceptAsync(sender.Id,
                    new DuplexMessage($"[{sender.Id}] {msg.PayloadString}"));
            };

            await server.StartAsync(port);

            // 3つのクライアント
            using var client1 = new TcpDuplexClient("localhost", port);
            using var client2 = new TcpDuplexClient("localhost", port);
            using var client3 = new TcpDuplexClient("localhost", port);

            client1.OnReceived += (ch, msg) => Debug.WriteLine($"[Client1] {msg.PayloadString}");
            client2.OnReceived += (ch, msg) => Debug.WriteLine($"[Client2] {msg.PayloadString}");
            client3.OnReceived += (ch, msg) => Debug.WriteLine($"[Client3] {msg.PayloadString}");

            await client1.ConnectAsync();
            await Task.Delay(50);
            await client2.ConnectAsync();
            await Task.Delay(50);
            await client3.ConnectAsync();
            await Task.Delay(50);

            Debug.WriteLine($"\n[Server] Connected clients: {server.Clients.Length}\n");

            // Client1が送信 → Client2, Client3が受信
            await client1.SendAsync("Hello from Client1!");
            await Task.Delay(100);

            // Client2が送信 → Client1, Client3が受信
            await client2.SendAsync("Hi from Client2!");
            await Task.Delay(100);

            await client1.CloseAsync();
            await client2.CloseAsync();
            await client3.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// 並列送信の例（双方向で同時にデータを投げ合う）
        /// </summary>
        public static async Task ParallelSendExample()
        {
            const int port = 12347;

            using var server = new TcpDuplexServer();
            var serverReceiveCount = 0;
            var clientReceiveCount = 0;

            server.OnReceived += (client, msg) =>
            {
                Interlocked.Increment(ref serverReceiveCount);
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);

            client.OnReceived += (ch, msg) =>
            {
                Interlocked.Increment(ref clientReceiveCount);
            };

            await client.ConnectAsync();
            await Task.Delay(100);

            // 双方向で同時に投げ合う
            var clientTasks = new Task[10];
            var serverTasks = new Task[10];

            for (int i = 0; i < 10; i++)
            {
                var index = i;
                clientTasks[i] = client.SendAsync($"Client message {index}");
                serverTasks[i] = server.BroadcastAsync($"Server message {index}");
            }

            await Task.WhenAll(clientTasks);
            await Task.WhenAll(serverTasks);

            await Task.Delay(500);

            Debug.WriteLine($"[Result] Server received: {serverReceiveCount} messages");
            Debug.WriteLine($"[Result] Client received: {clientReceiveCount} messages");

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// リクエスト/レスポンスの並列実行例
        /// </summary>
        public static async Task ParallelRequestExample()
        {
            const int port = 12348;

            using var server = new TcpDuplexServer();

            // サーバー：リクエストにエコー応答（少し遅延あり）
            server.OnReceived += async (client, msg) =>
            {
                if (msg.Type == MessageType.Request)
                {
                    await Task.Delay(50); // 処理時間シミュレート
                    await client.ReplyAsync(msg, $"Response to: {msg.PayloadString}");
                }
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();

            // 5つのリクエストを並列実行
            var tasks = new Task<DuplexMessage>[5];
            for (int i = 0; i < 5; i++)
            {
                tasks[i] = client.SendAndReceiveAsync($"Request {i}");
            }

            // 全部待つ
            var results = await Task.WhenAll(tasks);

            Debug.WriteLine("\n[Parallel Request Results]");
            for (int i = 0; i < results.Length; i++)
            {
                Debug.WriteLine($"  {i}: {results[i].PayloadString}");
            }

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// ネットワーク越しの通信例（別マシンからの接続想定）
        /// </summary>
        public static async Task NetworkExample()
        {
            const int port = 12349;

            Debug.WriteLine("=== Network Communication Example ===");
            Debug.WriteLine($"Port: {port}");
            Debug.WriteLine("");

            // サーバー起動
            using var server = new TcpDuplexServer();

            server.OnClientConnected += (client) =>
            {
                Debug.WriteLine($"Remote client connected: {client.Id}");
            };

            server.OnReceived += async (client, msg) =>
            {
                Debug.WriteLine($"Received: {msg.PayloadString}");

                // リクエストに応答
                if (msg.Type == MessageType.Request)
                {
                    await client.ReplyAsync(msg, $"Server processed: {msg.PayloadString}");
                }
            };

            await server.StartAsync(port);
            Debug.WriteLine("Server started. Waiting for connections...");
            Debug.WriteLine("(In production, another machine would connect here)");
            Debug.WriteLine("" );

            // デモ用にローカルクライアントで接続
            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();
            Debug.WriteLine("Demo client connected.");

            var response = await client.SendAndReceiveAsync("Hello from another machine!");
            Debug.WriteLine($"Response: {response.PayloadString}");

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// バイナリデータ送信の例
        /// </summary>
        public static async Task BinaryDataExample()
        {
            const int port = 12350;

            using var server = new TcpDuplexServer();

            server.OnReceived += async (client, msg) =>
            {
                Debug.WriteLine($"[Server] Received {msg.Payload?.Length ?? 0} bytes");

                // バイナリデータを加工して返す
                if (msg.Payload != null && msg.Type == MessageType.Request)
                {
                    var processed = new byte[msg.Payload.Length];
                    for (int i = 0; i < msg.Payload.Length; i++)
                    {
                        processed[i] = (byte)(msg.Payload[i] ^ 0xFF); // 反転
                    }
                    await client.ReplyAsync(msg, new DuplexMessage(processed));
                }
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();

            // バイナリデータ送信
            var data = new byte[] { 0x00, 0x11, 0x22, 0x33, 0xFF };
            var response = await client.SendAndReceiveAsync(new DuplexMessage(data));

            Debug.WriteLine($"Original: {BitConverter.ToString(data)}");
            Debug.WriteLine($"Response: {BitConverter.ToString(response.Payload)}");

            await client.CloseAsync();
            await server.StopAsync();
        }

        /// <summary>
        /// タグを使ったメッセージルーティングの例
        /// </summary>
        public static async Task TaggedMessageExample()
        {
            const int port = 12351;

            using var server = new TcpDuplexServer();

            server.OnReceived += async (client, msg) =>
            {
                // タグでルーティング
                switch (msg.Tag)
                {
                    case "echo":
                        await client.ReplyAsync(msg, $"Echo: {msg.PayloadString}");
                        break;

                    case "broadcast":
                        await server.BroadcastAsync(msg);
                        break;

                    case "ping":
                        await client.ReplyAsync(msg, "pong");
                        break;

                    default:
                        Debug.WriteLine($"Unknown tag: {msg.Tag}");
                        break;
                }
            };

            await server.StartAsync(port);

            using var client = new TcpDuplexClient("localhost", port);
            await client.ConnectAsync();

            // タグ付きリクエスト
            var echoMsg = new DuplexMessage("Hello") { Tag = "echo" };
            var response = await client.SendAndReceiveAsync(echoMsg);
            Debug.WriteLine($"Echo response: {response.PayloadString}");

            var pingMsg = new DuplexMessage("") { Tag = "ping" };
            response = await client.SendAndReceiveAsync(pingMsg);
            Debug.WriteLine($"Ping response: {response.PayloadString}");

            await client.CloseAsync();
            await server.StopAsync();
        }
    }
}
