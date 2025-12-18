using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using HagLib.NET.Duplex;

namespace HagLib.NET.Duplex.Examples
{
    /// <summary>
    /// 名前付きパイプ双方向通信の使用例
    /// </summary>
    public static class PipeExample
    {
        /// <summary>
        /// 基本的な双方向通信の例
        /// </summary>
        public static async Task BasicExample()
        {
            const string pipeName = "TestDuplexPipe";

            // ========== サーバー側 ==========
            using var server = new PipeDuplexServer(pipeName);

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

            await server.StartAsync();
            Debug.WriteLine($"[Server] Listening on pipe: {pipeName}");

            // ========== クライアント側 ==========
            using var client = new PipeDuplexClient(pipeName);

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
            const string pipeName = "MultiClientPipe";

            using var server = new PipeDuplexServer(pipeName, maxClients: 4);

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

            await server.StartAsync();

            // 3つのクライアント
            using var client1 = new PipeDuplexClient(pipeName);
            using var client2 = new PipeDuplexClient(pipeName);
            using var client3 = new PipeDuplexClient(pipeName);

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
            const string pipeName = "ParallelPipe";

            using var server = new PipeDuplexServer(pipeName);
            var serverReceiveCount = 0;
            var clientReceiveCount = 0;

            server.OnReceived += (client, msg) =>
            {
                Interlocked.Increment(ref serverReceiveCount);
            };

            await server.StartAsync();

            using var client = new PipeDuplexClient(pipeName);

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
        /// プロセス間通信の例（別プロセスからの接続想定）
        /// </summary>
        public static async Task InterProcessExample()
        {
            // このパイプ名を別プロセスでも使用することで
            // プロセス間通信が可能
            const string pipeName = "MyApp_InterProcess_Pipe";

            Debug.WriteLine("=== Inter-Process Communication Example ===");
            Debug.WriteLine($"Pipe name: {pipeName}");
            Debug.WriteLine("");

            // サーバー起動
            using var server = new PipeDuplexServer(pipeName);

            server.OnClientConnected += (client) =>
            {
                Debug.WriteLine($"External process connected: {client.Id}");
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

            await server.StartAsync();
            Debug.WriteLine("Server started. Waiting for connections...");
            Debug.WriteLine("(In production, another process would connect here)");
            Debug.WriteLine("");

            // デモ用にローカルクライアントで接続
            using var client = new PipeDuplexClient(pipeName);
            await client.ConnectAsync();
            Debug.WriteLine("Demo client connected.");

            var response = await client.SendAndReceiveAsync("Hello from another process!");
            Debug.WriteLine($"Response: {response.PayloadString}");

            await client.CloseAsync();
            await server.StopAsync();
        }
    }
}
