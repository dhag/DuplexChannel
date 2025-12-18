using System;
using System.Diagnostics;
using System.Threading.Tasks;
using HagLib.NET.Duplex;

namespace HagLib.NET.Duplex.Examples
{
    public class WebSocketServerExample
    {

        static WebSocketDuplexServer server;
        public static async Task Main(string[] args)
        {
            //var port =  8080;
            server = new WebSocketDuplexServer();

            // クライアント接続時
            server.OnClientConnected += (client) =>
            {
                Debug.WriteLine($"[Server] Client connected: {client.Id}");
            };

            // クライアント切断時
            server.OnClientDisconnected += (client) =>
            {
                Debug.WriteLine($"[Server] Client disconnected: {client.Id}");
            };

            // メッセージ受信時
            server.OnReceived += async (client, msg) =>
            {
                var payload = msg.ToTypedPayload();

                Debug.WriteLine($"[Server] Received from {client.Id}:");
                Debug.WriteLine($"  Text: {payload.GetText()}");
                Debug.WriteLine($"  JSON: {payload.GetJson()}");

                // 応答を返す
                var response = new TypedPayload()
                    .AddText("受信しました！")
                    .AddJson($"{{\"clientId\":\"{client.Id}\",\"itemCount\":{payload.Count}}}");

                await client.ReplyAsync(msg, response.ToMessage());
            };

            // サーバー起動
            //await server.StartAsync(port);
            await server.StartAsync("http://localhost:8080/");
            //Debug.WriteLine($"[Server] Listening on ws://localhost:{port}/");
            //Debug.WriteLine("[Server] Press Enter to stop...");

            //Console.ReadLine();

            //await server.StopAsync();
            //Debug.WriteLine("[Server] Stopped.");
        }

        public static async Task Stop()
        {
            await server.StopAsync();
            Debug.WriteLine("[Server] Stopped.");

        }
    }
}
