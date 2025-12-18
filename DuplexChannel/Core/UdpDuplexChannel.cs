using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace HagLib.NET.Duplex
{
    /// <summary>
    /// UDP双方向通信チャネル
    /// 
    /// 注意:
    /// - コネクションレス（接続状態なし）
    /// - パケットロス・順序逆転の可能性あり
    /// - 1パケット最大約1400バイト推奨（MTU制限）
    /// - SendAndReceiveAsync は応答が届かない可能性あり（タイムアウト必須）
    /// </summary>
    public class UdpDuplexChannel : IDisposable
    {
        private UdpClient _udpClient;
        private readonly CancellationTokenSource _cts;
        private readonly ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>> _pendingRequests;
        private readonly SemaphoreSlim _sendLock;
        private int _nextMessageId;
        private bool _disposed;
        private Task _receiveTask;
        private IPEndPoint _remoteEndPoint;
        private IPEndPoint _localEndPoint;

        /// <summary>受信時の送信元アドレス</summary>
        public IPEndPoint LastReceivedFrom { get; private set; }

        public string Id { get; }

        /// <summary>メッセージ受信イベント</summary>
        public event Action<UdpDuplexChannel, DuplexMessage, IPEndPoint> OnReceived;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public UdpDuplexChannel()
        {
            _cts = new CancellationTokenSource();
            _pendingRequests = new ConcurrentDictionary<int, TaskCompletionSource<DuplexMessage>>();
            _sendLock = new SemaphoreSlim(1, 1);
            _nextMessageId = 0;
            Id = Guid.NewGuid().ToString("N").Substring(0, 8);
        }

        /// <summary>
        /// 受信用にバインド（サーバー的な使い方）
        /// </summary>
        public void Bind(int port)
        {
            Bind(new IPEndPoint(IPAddress.Any, port));
        }

        /// <summary>
        /// 受信用にバインド
        /// </summary>
        public void Bind(IPEndPoint localEndPoint)
        {
            _localEndPoint = localEndPoint;
            _udpClient = new UdpClient(localEndPoint);
            StartReceiving();
        }

        /// <summary>
        /// 送信先を設定（クライアント的な使い方）
        /// </summary>
        public void Connect(string host, int port)
        {
            Connect(new IPEndPoint(IPAddress.Parse(host), port));
        }

        /// <summary>
        /// 送信先を設定
        /// </summary>
        public void Connect(IPEndPoint remoteEndPoint)
        {
            _remoteEndPoint = remoteEndPoint;
            if (_udpClient == null)
            {
                _udpClient = new UdpClient();
            }
            StartReceiving();
        }

        /// <summary>
        /// バインドと送信先設定を両方行う
        /// </summary>
        public void BindAndConnect(int localPort, string remoteHost, int remotePort)
        {
            _localEndPoint = new IPEndPoint(IPAddress.Any, localPort);
            _remoteEndPoint = new IPEndPoint(IPAddress.Parse(remoteHost), remotePort);
            _udpClient = new UdpClient(_localEndPoint);
            StartReceiving();
        }

        private void StartReceiving()
        {
            if (_receiveTask == null)
            {
                _receiveTask = Task.Run(() => ReceiveLoopAsync(_cts.Token));
            }
        }

        /// <summary>
        /// プッシュ送信（デフォルト送信先へ）
        /// </summary>
        public Task SendAsync(DuplexMessage message, CancellationToken ct = default)
        {
            if (_remoteEndPoint == null)
                throw new InvalidOperationException("Remote endpoint not set. Call Connect() first.");
            return SendAsync(message, _remoteEndPoint, ct);
        }

        /// <summary>
        /// プッシュ送信（送信先指定）
        /// </summary>
        public async Task SendAsync(DuplexMessage message, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            await SendInternalAsync(message, remoteEndPoint, ct).ConfigureAwait(false);
        }

        public Task SendAsync(string text, CancellationToken ct = default)
        {
            return SendAsync(new DuplexMessage(text), ct);
        }

        public Task SendAsync(byte[] data, CancellationToken ct = default)
        {
            return SendAsync(new DuplexMessage(data), ct);
        }

        public Task SendAsync(string text, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            return SendAsync(new DuplexMessage(text), remoteEndPoint, ct);
        }

        /// <summary>
        /// リクエスト送信して応答を待つ
        /// 注意: UDPは信頼性がないため、タイムアウト必須
        /// </summary>
        public async Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, int timeoutMs = 3000, CancellationToken ct = default)
        {
            if (_remoteEndPoint == null)
                throw new InvalidOperationException("Remote endpoint not set. Call Connect() first.");
            return await SendAndReceiveAsync(message, _remoteEndPoint, timeoutMs, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// リクエスト送信して応答を待つ（送信先指定）
        /// </summary>
        public async Task<DuplexMessage> SendAndReceiveAsync(DuplexMessage message, IPEndPoint remoteEndPoint, int timeoutMs = 3000, CancellationToken ct = default)
        {
            message.Type = MessageType.Request;
            message.Id = Interlocked.Increment(ref _nextMessageId);

            var tcs = new TaskCompletionSource<DuplexMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pendingRequests[message.Id] = tcs;

            try
            {
                using var timeoutCts = new CancellationTokenSource(timeoutMs);
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token, timeoutCts.Token);
                linkedCts.Token.Register(() => tcs.TrySetCanceled());

                await SendInternalAsync(message, remoteEndPoint, linkedCts.Token).ConfigureAwait(false);
                return await tcs.Task.ConfigureAwait(false);
            }
            finally
            {
                _pendingRequests.TryRemove(message.Id, out _);
            }
        }

        public Task<DuplexMessage> SendAndReceiveAsync(string text, int timeoutMs = 3000, CancellationToken ct = default)
        {
            return SendAndReceiveAsync(new DuplexMessage(text), timeoutMs, ct);
        }

        /// <summary>
        /// リクエストに応答
        /// </summary>
        public Task ReplyAsync(DuplexMessage request, DuplexMessage response, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            response.Type = MessageType.Response;
            response.Id = request.Id;
            return SendInternalAsync(response, remoteEndPoint, ct);
        }

        public Task ReplyAsync(DuplexMessage request, string text, IPEndPoint remoteEndPoint, CancellationToken ct = default)
        {
            return ReplyAsync(request, new DuplexMessage(text), remoteEndPoint, ct);
        }

        /// <summary>
        /// ブロードキャスト送信
        /// </summary>
        public async Task BroadcastAsync(DuplexMessage message, int port, CancellationToken ct = default)
        {
            message.Type = MessageType.Push;
            message.Id = Interlocked.Increment(ref _nextMessageId);
            var broadcastEndPoint = new IPEndPoint(IPAddress.Broadcast, port);
            
            // ブロードキャスト許可
            _udpClient.EnableBroadcast = true;
            await SendInternalAsync(message, broadcastEndPoint, ct).ConfigureAwait(false);
        }

        public Task BroadcastAsync(string text, int port, CancellationToken ct = default)
        {
            return BroadcastAsync(new DuplexMessage(text), port, ct);
        }

        public void Close()
        {
            _cts.Cancel();
            _udpClient?.Close();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _cts.Cancel();
            _udpClient?.Close();
            _udpClient?.Dispose();
            _cts.Dispose();
            _sendLock?.Dispose();
        }

        private async Task SendInternalAsync(DuplexMessage message, IPEndPoint remoteEndPoint, CancellationToken ct)
        {
            await _sendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var packet = DuplexPacket.Serialize(message);
                
                // MTU警告（大きすぎるとフラグメント化される）
                if (packet.Length > 1400)
                {
                    System.Diagnostics.Debug.WriteLine($"Warning: UDP packet size {packet.Length} exceeds recommended MTU");
                }

                await _udpClient.SendAsync(packet, packet.Length, remoteEndPoint).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var result = await _udpClient.ReceiveAsync().ConfigureAwait(false);
                    
                    if (ct.IsCancellationRequested) break;

                    LastReceivedFrom = result.RemoteEndPoint;
                    var data = result.Buffer;

                    if (data.Length < DuplexPacket.HeaderSize)
                        continue;

                    // ヘッダー解析
                    var header = new byte[DuplexPacket.HeaderSize];
                    Buffer.BlockCopy(data, 0, header, 0, DuplexPacket.HeaderSize);

                    if (!DuplexPacket.TryParseHeader(header, out var type, out var messageId, out var payloadLength, out var tagLength))
                        continue;

                    // ボディ解析
                    var bodyLength = tagLength + payloadLength;
                    byte[] body = null;
                    if (bodyLength > 0 && data.Length >= DuplexPacket.HeaderSize + bodyLength)
                    {
                        body = new byte[bodyLength];
                        Buffer.BlockCopy(data, DuplexPacket.HeaderSize, body, 0, bodyLength);
                    }

                    var message = DuplexPacket.ParseBody(type, messageId, body ?? Array.Empty<byte>(), tagLength);
                    HandleMessage(message, result.RemoteEndPoint);
                }
            }
            catch (ObjectDisposedException) { }
            catch (SocketException) { }
            catch (OperationCanceledException) { }
            finally
            {
                foreach (var kvp in _pendingRequests)
                {
                    kvp.Value.TrySetCanceled();
                }
                _pendingRequests.Clear();
            }
        }

        private void HandleMessage(DuplexMessage message, IPEndPoint from)
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
                OnReceived?.Invoke(this, message, from);
            }
        }
    }
}
