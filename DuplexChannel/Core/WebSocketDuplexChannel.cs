using System;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// WebSocket双方向通信チャネル（コア通信機能）
    /// サーバー・クライアント両方で使用
    /// </summary>
    public class WebSocketDuplexChannel : IDuplexChannel
    {
        private readonly WebSocket _webSocket;
        private readonly ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>> _pendingRequests;
        private readonly SemaphoreSlim _sendLock;
        private int _nextMessageId;
        private bool _disposed;
        private bool _receiveLoopStarted;

        public bool IsConnected => _webSocket?.State == WebSocketState.Open;
        public string Id { get; }

        public event Action<IDuplexChannel, DuplexMessage> OnReceived;
        public event Action<IDuplexChannel> OnDisconnected;

        /// <summary>
        /// WebSocketからチャネルを作成
        /// </summary>
        public WebSocketDuplexChannel(WebSocket webSocket, string id = null)
        {
            _webSocket = webSocket ?? throw new ArgumentNullException(nameof(webSocket));
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            Id = id ?? Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        /// <summary>
        /// 受信ループを開始（サーバー/クライアント共通）
        /// </summary>
        public void StartReceiving(CancellationToken ct = default)
        {
            if (_receiveLoopStarted)
                throw new InvalidOperationException("Receive loop already started.");

            _receiveLoopStarted = true;
            _ = ReceiveLoopAsync(ct);
        }

        #region IDuplexChannel 実装

        public Task SendAsync(DuplexMessage message, CancellationToken ct = default)
        {
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            return SendInternalAsync(message, ct);
        }

        public Task SendAsync(string text, CancellationToken ct = default)
            => SendAsync(new DuplexMessage(text), ct);

        public Task SendAsync(byte[] data, CancellationToken ct = default)
            => SendAsync(new DuplexMessage(data), ct);

        public async Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, CancellationToken ct = default)
        {
            message.Type = MessageType.Request;
            message.Id = Interlocked.Increment(ref _nextMessageId);

            var tcs = new TaskCompletionSource<DuplexMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[message.Id] = tcs;

            try
            {
                using var registration = ct.Register(() => tcs.TrySetCanceled());
                await SendInternalAsync(message, ct).ConfigureAwait(false);
                return await tcs.Task.ConfigureAwait(false);
            }
            finally
            {
                _pendingRequests.TryRemove(message.Id, out _);
            }
        }

        public Task<DuplexMessage> SendAndReceiveAsync(string text, CancellationToken ct = default)
            => SendAndReceiveAsync(new DuplexMessage(text), ct);

        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, CancellationToken ct = default)
        {
            response.Type = MessageType.Response;
            response.Id = request.Id;
            return SendInternalAsync(response, ct);
        }

        public Task ReplyAsync(DuplexMessage request, string text, CancellationToken ct = default)
            => ReplyAsync(request, new DuplexMessage(text), ct);

        public async Task CloseAsync()
        {
            if (_disposed) return;

            try
            {
                if (_webSocket.State == WebSocketState.Open)
                {
                    await _webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing", CancellationToken.None)
                        .ConfigureAwait(false);
                }
            }
            catch { }

            NotifyDisconnected();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            try { _webSocket?.Dispose(); } catch { }
            _sendLock?.Dispose();

            foreach (var kvp in _pendingRequests)
            {
                kvp.Value.TrySetCanceled();
            }
            _pendingRequests.Clear();
        }

        #endregion

        #region 追加メソッド（TypedPayload対応）

        public Task SendAsync(TypedPayload payload, CancellationToken ct = default)
            => SendAsync(payload.ToMessage(), ct);

        public async Task<TypedPayload> SendAndReceiveAsync(TypedPayload payload, CancellationToken ct = default)
        {
            var response = await SendAndReceiveAsync(payload.ToMessage(), ct).ConfigureAwait(false);
            return response.ToTypedPayload();
        }

        public Task ReplyAsync(DuplexMessage request, TypedPayload payload, CancellationToken ct = default)
            => ReplyAsync(request, payload.ToMessage(), ct);

        #endregion

        #region 内部実装

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            var buffer = new byte[8192];

            try
            {
                while (!ct.IsCancellationRequested && _webSocket.State == WebSocketState.Open)
                {
                    // 複数フレームを結合して完全なメッセージを受信
                    using var ms = new System.IO.MemoryStream();
                    WebSocketReceiveResult result;

                    do
                    {
                        result = await _webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), ct).ConfigureAwait(false);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            return; // ループ終了
                        }

                        ms.Write(buffer, 0, result.Count);
                    } while (!result.EndOfMessage); // 全フレーム受信まで繰り返す

                    var completeData = ms.ToArray();

                    if (result.MessageType == WebSocketMessageType.Text)
                    {
                        var json = Encoding.UTF8.GetString(completeData);
                        HandleJsonMessage(json);
                    }
                    else if (result.MessageType == WebSocketMessageType.Binary)
                    {
                        HandleBinaryMessage(completeData);
                    }
                }
            }
            catch (WebSocketException) { }
            catch (OperationCanceledException) { }
            finally
            {
                if (!_disposed)
                {
                    NotifyDisconnected();
                }

                foreach (var kvp in _pendingRequests)
                {
                    kvp.Value.TrySetCanceled();
                }
                _pendingRequests.Clear();
            }
        }

        private void NotifyDisconnected()
        {
            try
            {
                OnDisconnected?.Invoke(this);
            }
            catch { }
        }

        private void HandleJsonMessage(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                var msgType = root.TryGetProperty("type", out var typeEl) ? typeEl.GetString() : "send";
                var id = root.TryGetProperty("id", out var idEl) ? idEl.GetInt32() : 0;

                TypedPayload payload;

                if (root.TryGetProperty("items", out var itemsEl) && itemsEl.ValueKind == JsonValueKind.Array)
                {
                    payload = ParseItemsFromJson(itemsEl);
                }
                else if (root.TryGetProperty("text", out var textEl))
                {
                    payload = TypedPayload.FromText(textEl.GetString());
                }
                else if (root.TryGetProperty("json", out var jsonEl))
                {
                    payload = TypedPayload.FromJson(jsonEl.GetRawText());
                }
                else
                {
                    payload = new TypedPayload();
                }

                var message = new DuplexMessage
                {
                    Id = id,
                    Type = msgType == "request" ? MessageType.Request :
                           msgType == "response" ? MessageType.Response : MessageType.Push,
                    Payload = payload.Serialize()
                };

                HandleMessage(message);
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"JSON parse error: {ex.Message}");
            }
        }

        private TypedPayload ParseItemsFromJson(JsonElement itemsEl)
        {
            var payload = new TypedPayload();

            foreach (var item in itemsEl.EnumerateArray())
            {
                var type = item.TryGetProperty("type", out var typeEl) ? typeEl.GetInt32() : 0;
                var mimeType = item.TryGetProperty("mimeType", out var mimeEl) ? mimeEl.GetString() : "";
                var data = item.TryGetProperty("data", out var dataEl) ? dataEl.GetString() : "";
                var encoding = item.TryGetProperty("encoding", out var encEl) ? encEl.GetString() : "";

                byte[] bytes;
                if (encoding == "base64")
                {
                    bytes = Convert.FromBase64String(data);
                }
                else
                {
                    bytes = Encoding.UTF8.GetBytes(data);
                }

                switch ((ContentType)type)
                {
                    case ContentType.Text:
                        payload.AddText(data);
                        break;
                    case ContentType.Json:
                        payload.AddJson(data);
                        break;
                    case ContentType.Image:
                        payload.AddImage(bytes, mimeType ?? "image/png");
                        break;
                    case ContentType.Binary:
                        payload.AddBinary(bytes, mimeType ?? "application/octet-stream");
                        break;
                    default:
                        payload.AddCustom(bytes, mimeType ?? "application/octet-stream");
                        break;
                }
            }

            return payload;
        }

        private void HandleBinaryMessage(byte[] data)
        {
            if (data.Length < DuplexPacket.HeaderSize)
                return;

            var header = new byte[DuplexPacket.HeaderSize];
            Buffer.BlockCopy(data, 0, header, 0, DuplexPacket.HeaderSize);

            if (!DuplexPacket.TryParseHeader(header, out var type, out var messageId, out var payloadLength, out var tagLength))
                return;

            var bodyLength = tagLength + payloadLength;
            var body = new byte[bodyLength];
            if (bodyLength > 0 && data.Length >= DuplexPacket.HeaderSize + bodyLength)
            {
                Buffer.BlockCopy(data, DuplexPacket.HeaderSize, body, 0, bodyLength);
            }

            var message = DuplexPacket.ParseBody(type, messageId, body, tagLength);
            HandleMessage(message);
        }

        private void HandleMessage(DuplexMessage message)
        {
            if (message.Type == MessageType.Response)
            {
                if (_pendingRequests.TryRemove(message.Id, out var tcs))
                {
                    tcs.TrySetResult(message);
                }
            }
            else
            {
                OnReceived?.Invoke(this, message);
            }
        }

        private async Task SendInternalAsync(DuplexMessage message, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var payload = message.ToTypedPayload();
                var json = SerializeToJson(message, payload);
                var bytes = Encoding.UTF8.GetBytes(json);

                await _webSocket.SendAsync(
                    new ArraySegment<byte>(bytes),
                    WebSocketMessageType.Text,
                    true,
                    ct
                ).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private string SerializeToJson(DuplexMessage message, TypedPayload payload)
        {
            var sb = new StringBuilder();
            sb.Append("{");

            var typeStr = message.Type switch
            {
                MessageType.Push => "push",
                MessageType.Request => "request",
                MessageType.Response => "response",
                _ => "push"
            };
            sb.Append($"\"type\":\"{typeStr}\",");
            sb.Append($"\"id\":{message.Id},");

            sb.Append("\"items\":[");
            var first = true;
            foreach (var item in payload)
            {
                if (!first) sb.Append(",");
                first = false;

                sb.Append("{");
                sb.Append($"\"type\":{(int)item.Type},");
                sb.Append($"\"mimeType\":\"{EscapeJson(item.MimeType)}\",");

                if (item.Type == ContentType.Text || item.Type == ContentType.Json)
                {
                    sb.Append($"\"data\":\"{EscapeJson(item.DataString ?? "")}\"");
                }
                else
                {
                    sb.Append($"\"data\":\"{Convert.ToBase64String(item.Data ?? Array.Empty<byte>())}\",");
                    sb.Append("\"encoding\":\"base64\"");
                }

                sb.Append("}");
            }
            sb.Append("]");

            sb.Append("}");
            return sb.ToString();
        }

        private static string EscapeJson(string s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return s.Replace("\\", "\\\\")
                    .Replace("\"", "\\\"")
                    .Replace("\n", "\\n")
                    .Replace("\r", "\\r")
                    .Replace("\t", "\\t");
        }

        #endregion
    }
}