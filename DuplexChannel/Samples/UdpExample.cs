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
        /// 基本的な送受信の例
        /// </summary>
        public static async Task BasicExample()
        {
            const int serverPort = 22922;
            const int clientPort = 22923;

            // ========== 受信側（サーバー的） ==========
            using var receiver = new UdpDuplexChannel();

            receiver.OnReceived += async (ch, msg, from) =>
            {
                Debug.WriteLine($"[Receiver] From {from}: {msg.PayloadString}");

                // リクエストなら応答
                if (msg.Type == MessageType.Request)
                {
                    await ch.ReplyAsync(msg, $"Response to: {msg.PayloadString}", from);
                }
            };

            receiver.Bind(serverPort);
            Debug.WriteLine($"[Receiver] Listening on port {serverPort}");

            // ========== 送信側（クライアント的） ==========
            using var sender = new UdpDuplexChannel();

            sender.OnReceived += (ch, msg, from) =>
            {
                Debug.WriteLine($"[Sender] Received: {msg.PayloadString}");
            };

            sender.BindAndConnect(clientPort, "127.0.0.1", serverPort);
            Debug.WriteLine($"[Sender] Ready on port {clientPort}");
            // ========== 通信 ==========

            // プッシュ送信
            await sender.SendAsync("Hello UDP!");
            await Task.Delay(100);

            // リクエスト/レスポンス（タイムアウト付き）
            try
            {
                var response = await sender.SendAndReceiveAsync("Ping", timeoutMs: 3000);
                Debug.WriteLine($"[Sender] Response: {response.PayloadString}");
            }
            catch (OperationCanceledException)
            {
                Debug.WriteLine("[Sender] Request timed out");
            }

            sender.Close();
            receiver.Close();
        }

        /// <summary>
        /// ブロードキャストの例
        /// </summary>
        public static async Task BroadcastExample()
        {
            const int port = 22924;

            // 受信側を2つ起動
            using var receiver1 = new UdpDuplexChannel();
            using var receiver2 = new UdpDuplexChannel();

            receiver1.OnReceived += (ch, msg, from) =>
            {
                Debug.WriteLine($"[Receiver1] {msg.PayloadString}");
            };

            receiver2.OnReceived += (ch, msg, from) =>
            {
                Debug.WriteLine($"[Receiver2] {msg.PayloadString}");
            };

            // 同じポートで受信するには SO_REUSEADDR が必要
            // ここでは別ポートで示す
            receiver1.Bind(port);
            receiver2.Bind(port + 1);

            // 送信側
            using var sender = new UdpDuplexChannel();
            sender.Bind(port + 2);

            // ブロードキャスト（LAN内全員に届く）
            Debug.WriteLine("[Sender] Broadcasting...");
            await sender.BroadcastAsync("Hello everyone!", port);

            await Task.Delay(100);

            sender.Close();
            receiver1.Close();
            receiver2.Close();
        }

        /// <summary>
        /// 双方向通信の例（互いに送り合う）
        /// </summary>
        public static async Task BidirectionalExample()
        {
            const int portA = 22930;
            const int portB = 22931;

            // ノードA
            using var nodeA = new UdpDuplexChannel();
            var receivedByA = 0;

            nodeA.OnReceived += (ch, msg, from) =>
            {
                Interlocked.Increment(ref receivedByA);
                Debug.WriteLine($"[A] Received: {msg.PayloadString}");
            };

            nodeA.BindAndConnect(portA, "127.0.0.1", portB);

            // ノードB
            using var nodeB = new UdpDuplexChannel();
            var receivedByB = 0;

            nodeB.OnReceived += (ch, msg, from) =>
            {
                Interlocked.Increment(ref receivedByB);
                Debug.WriteLine($"[B] Received: {msg.PayloadString}");
            };

            nodeB.BindAndConnect(portB, "127.0.0.1", portA);

            await Task.Delay(100);

            // 双方向で同時に送信
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

            nodeA.Close();
            nodeB.Close();
        }

        /// <summary>
        /// 複数の送信先への送信例
        /// </summary>
        public static async Task MultiTargetExample()
        {
            const int portA = 22940;
            const int portB = 22941;
            const int portC = 22942;
            const int senderPort = 22943;

            // 3つの受信ノード
            using var nodeA = new UdpDuplexChannel();
            using var nodeB = new UdpDuplexChannel();
            using var nodeC = new UdpDuplexChannel();

            nodeA.OnReceived += (ch, msg, from) => Debug.WriteLine($"[A] {msg.PayloadString}");
            nodeB.OnReceived += (ch, msg, from) => Debug.WriteLine($"[B] {msg.PayloadString}");
            nodeC.OnReceived += (ch, msg, from) => Debug.WriteLine($"[C] {msg.PayloadString}");

            nodeA.Bind(portA);
            nodeB.Bind(portB);
            nodeC.Bind(portC);

            // 送信ノード（複数の宛先に送信）
            using var sender = new UdpDuplexChannel();
            sender.Bind(senderPort);

            var endPointA = new IPEndPoint(IPAddress.Loopback, portA);
            var endPointB = new IPEndPoint(IPAddress.Loopback, portB);
            var endPointC = new IPEndPoint(IPAddress.Loopback, portC);

            // 個別に送信
            await sender.SendAsync(new DuplexMessage("Hello A!"), endPointA);
            await sender.SendAsync(new DuplexMessage("Hello B!"), endPointB);
            await sender.SendAsync(new DuplexMessage("Hello C!"), endPointC);

            await Task.Delay(100);

            nodeA.Close();
            nodeB.Close();
            nodeC.Close();
            sender.Close();
        }

        /// <summary>
        /// 発見プロトコルの例（LAN内のサーバーを探す）
        /// </summary>
        public static async Task DiscoveryExample()
        {
            const int discoveryPort = 22950;

            // サーバー（発見要求に応答）
            using var server = new UdpDuplexChannel();

            server.OnReceived += async (ch, msg, from) =>
            {
                if (msg.PayloadString == "DISCOVER")
                {
                    Debug.WriteLine($"[Server] Discovery request from {from}");
                    await ch.ReplyAsync(msg, "SERVER_HERE:MyServer:12345", from);
                }
            };

            server.Bind(discoveryPort);
            Debug.WriteLine($"[Server] Waiting for discovery on port {discoveryPort}");

            // クライアント（サーバーを探す）
            using var client = new UdpDuplexChannel();

            client.OnReceived += (ch, msg, from) =>
            {
                Debug.WriteLine($"[Client] Found server at {from}: {msg.PayloadString}");
            };

            client.Bind(discoveryPort + 1);

            // ブロードキャストで探す
            Debug.WriteLine("[Client] Sending discovery broadcast...");
            await client.BroadcastAsync("DISCOVER", discoveryPort);

            await Task.Delay(500);

            // または直接リクエスト（ローカルテスト用）
            var serverEndPoint = new IPEndPoint(IPAddress.Loopback, discoveryPort);
            try
            {
                var response = await client.SendAndReceiveAsync(
                    new DuplexMessage("DISCOVER"), 
                    serverEndPoint, 
                    timeoutMs: 1000);
                Debug.WriteLine($"[Client] Direct response: {response.PayloadString}");
            }
            catch (OperationCanceledException)
            {
                Debug.WriteLine("[Client] Discovery timed out");
            }

            server.Close();
            client.Close();
        }
    }
}
