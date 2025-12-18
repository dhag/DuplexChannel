using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using HagLib.NET.Duplex;

namespace HagLib.NET.Duplex.Examples
{
    /// <summary>
    /// UDP双方向通信の使用例
    /// </summary>
    public static class UdpExample
    {
        /// <summary>
        /// 基本的な送受信の例（TCP/Pipe/WebSocketと同じ書き方）
        /// </summary>
        public static async Task BasicExample()
        {
            const int serverPort = 22922;
            const int clientPort = 22923;

            // ========== 受信側（サーバー的） ==========
            using var receiver = new UdpDuplexChannel();

            // TCP/Pipe/WebSocketと同じイベント形式
            receiver.OnReceived += async (ch, msg) =>
            {
                Debug.WriteLine($"[Receiver] {msg.PayloadString}");

                // リクエストなら応答（LastReceivedFromに自動返信）
                if (msg.Type == MessageType.Request)
                {
                    await ch.ReplyAsync(msg, $"Response to: {msg.PayloadString}");
                }
            };

            receiver.Bind(serverPort);
            Debug.WriteLine($"[Receiver] Listening on port {serverPort}");

            // ========== 送信側（クライアント的） ==========
            using var sender = new UdpDuplexChannel();

            sender.OnReceived += (ch, msg) =>
            {
                Debug.WriteLine($"[Sender] Received: {msg.PayloadString}");
            };

            sender.BindAndConnect(clientPort, "127.0.0.1", serverPort);
            Debug.WriteLine($"[Sender] Ready on port {clientPort}");

            // ========== 通信（TCP/Pipe/WebSocketと同じ） ==========

            // プッシュ送信
            await sender.SendAsync("Hello UDP!");
            await Task.Delay(100);

            // リクエスト/レスポンス
            try
            {
                var response = await sender.SendAndReceiveAsync("Ping");
                Debug.WriteLine($"[Sender] Response: {response.PayloadString}");
            }
            catch (OperationCanceledException)
            {
                Debug.WriteLine("[Sender] Request timed out");
            }

            await sender.CloseAsync();
            await receiver.CloseAsync();
        }

        /// <summary>
        /// IDuplexChannel共通インターフェースで使う例
        /// </summary>
        public static async Task CommonInterfaceExample()
        {
            const int serverPort = 22925;
            const int clientPort = 22926;

            // サーバー
            var serverUdp = new UdpDuplexChannel();
            serverUdp.Bind(serverPort);

            IDuplexChannel server = serverUdp;
            server.OnReceived += async (ch, msg) =>
            {
                Debug.WriteLine($"[Server] Received: {msg.PayloadString}");

                if (msg.Type == MessageType.Request)
                {
                    await ch.ReplyAsync(msg, "OK from server");
                }
            };

            // クライアント
            var clientUdp = new UdpDuplexChannel();
            clientUdp.BindAndConnect(clientPort, "127.0.0.1", serverPort);

            IDuplexChannel client = clientUdp;

            // 共通インターフェースで通信（TCP/Pipe/WebSocketと完全に同じ）
            await client.SendAsync("Hello via IDuplexChannel");
            await Task.Delay(100);

            try
            {
                var response = await client.SendAndReceiveAsync("Ping via interface");
                Debug.WriteLine($"[Client] Response: {response.PayloadString}");
            }
            catch (OperationCanceledException)
            {
                Debug.WriteLine("[Client] Timeout");
            }

            await client.CloseAsync();
            await server.CloseAsync();
        }

        /// <summary>
        /// ブロードキャストの例（UDP固有機能）
        /// </summary>
        public static async Task BroadcastExample()
        {
            const int port = 22924;

            using var receiver1 = new UdpDuplexChannel();
            using var receiver2 = new UdpDuplexChannel();

            receiver1.OnReceived += (ch, msg) =>
            {
                Debug.WriteLine($"[Receiver1] {msg.PayloadString}");
            };

            receiver2.OnReceived += (ch, msg) =>
            {
                Debug.WriteLine($"[Receiver2] {msg.PayloadString}");
            };

            receiver1.Bind(port);
            receiver2.Bind(port + 1);

            using var sender = new UdpDuplexChannel();
            sender.Bind(port + 2);

            Debug.WriteLine("[Sender] Broadcasting...");
            await sender.BroadcastAsync("Hello everyone!", port);

            await Task.Delay(100);

            await sender.CloseAsync();
            await receiver1.CloseAsync();
            await receiver2.CloseAsync();
        }

        /// <summary>
        /// 双方向通信の例（互いに送り合う）
        /// </summary>
        public static async Task BidirectionalExample()
        {
            const int portA = 22930;
            const int portB = 22931;

            using var nodeA = new UdpDuplexChannel();
            var receivedByA = 0;

            nodeA.OnReceived += (ch, msg) =>
            {
                Interlocked.Increment(ref receivedByA);
                Debug.WriteLine($"[A] Received: {msg.PayloadString}");
            };

            nodeA.BindAndConnect(portA, "127.0.0.1", portB);

            using var nodeB = new UdpDuplexChannel();
            var receivedByB = 0;

            nodeB.OnReceived += (ch, msg) =>
            {
                Interlocked.Increment(ref receivedByB);
                Debug.WriteLine($"[B] Received: {msg.PayloadString}");
            };

            nodeB.BindAndConnect(portB, "127.0.0.1", portA);

            await Task.Delay(100);

            Debug.WriteLine("\n[Test] Bidirectional parallel send:");

            var tasksA = new Task[5];
            var tasksB = new Task[5];

            for (int i = 0; i < 5; i++)
            {
                var idx = i;
                tasksA[i] = nodeA.SendAsync($"From A: {idx}");
                tasksB[i] = nodeB.SendAsync($"From B: {idx}");
            }

            await Task.WhenAll(tasksA);
            await Task.WhenAll(tasksB);

            await Task.Delay(500);

            Debug.WriteLine($"\n[Result] A received: {receivedByA}, B received: {receivedByB}");

            await nodeA.CloseAsync();
            await nodeB.CloseAsync();
        }

        /// <summary>
        /// 複数の送信先への送信例（UDP固有：送信先指定版）
        /// </summary>
        public static async Task MultiTargetExample()
        {
            const int portA = 22940;
            const int portB = 22941;
            const int portC = 22942;
            const int senderPort = 22943;

            using var nodeA = new UdpDuplexChannel();
            using var nodeB = new UdpDuplexChannel();
            using var nodeC = new UdpDuplexChannel();

            nodeA.OnReceived += (ch, msg) => Debug.WriteLine($"[A] {msg.PayloadString}");
            nodeB.OnReceived += (ch, msg) => Debug.WriteLine($"[B] {msg.PayloadString}");
            nodeC.OnReceived += (ch, msg) => Debug.WriteLine($"[C] {msg.PayloadString}");

            nodeA.Bind(portA);
            nodeB.Bind(portB);
            nodeC.Bind(portC);

            using var sender = new UdpDuplexChannel();
            sender.Bind(senderPort);

            var endPointA = new IPEndPoint(IPAddress.Loopback, portA);
            var endPointB = new IPEndPoint(IPAddress.Loopback, portB);
            var endPointC = new IPEndPoint(IPAddress.Loopback, portC);

            // 送信先指定版（UDP固有）
            await sender.SendAsync(new DuplexMessage("Hello A!"), endPointA);
            await sender.SendAsync(new DuplexMessage("Hello B!"), endPointB);
            await sender.SendAsync(new DuplexMessage("Hello C!"), endPointC);

            await Task.Delay(100);

            await nodeA.CloseAsync();
            await nodeB.CloseAsync();
            await nodeC.CloseAsync();
            await sender.CloseAsync();
        }

        /// <summary>
        /// 発見プロトコルの例（LAN内のサーバーを探す）
        /// </summary>
        public static async Task DiscoveryExample()
        {
            const int discoveryPort = 22950;

            using var server = new UdpDuplexChannel();

            server.OnReceived += async (ch, msg) =>
            {
                if (msg.PayloadString == "DISCOVER")
                {
                    Debug.WriteLine($"[Server] Discovery request received");
                    await ch.ReplyAsync(msg, "SERVER_HERE:MyServer:12345");
                }
            };

            server.Bind(discoveryPort);
            Debug.WriteLine($"[Server] Waiting for discovery on port {discoveryPort}");

            using var client = new UdpDuplexChannel();

            client.OnReceived += (ch, msg) =>
            {
                Debug.WriteLine($"[Client] Found server: {msg.PayloadString}");
            };

            client.BindAndConnect(discoveryPort + 1, "127.0.0.1", discoveryPort);

            // ブロードキャストで探す
            Debug.WriteLine("[Client] Sending discovery broadcast...");
            await client.BroadcastAsync("DISCOVER", discoveryPort);

            await Task.Delay(500);

            // 直接リクエスト
            try
            {
                var response = await client.SendAndReceiveAsync("DISCOVER");
                Debug.WriteLine($"[Client] Direct response: {response.PayloadString}");
            }
            catch (OperationCanceledException)
            {
                Debug.WriteLine("[Client] Discovery timed out");
            }

            await server.CloseAsync();
            await client.CloseAsync();
        }
    }
}