使用例（Windows上でC#サーバーに接続）:
python# Python
from haglib_duplex import PipeDuplexClient

client = PipeDuplexClient("MyPipeName")  # C#サーバーのパイプ名
await client.connect()
response = await client.send_and_receive_text("Hello!")
javascript// Node.js
const { PipeDuplexClient } = require('./haglib_duplex.js');

const client = new PipeDuplexClient('MyPipeName');  // C#サーバーのパイプ名
await client.connect();
const response = await client.sendAndReceiveText('Hello!');
注意点:

Python版 - Windows APIをctypesで直接呼び出すため、追加ライブラリ不要
Node.js版 - net.connect()がWindows Named Pipesをネイティブサポート
パイプ名だけ指定 - \\.\pipe\プレフィックスは自動付加されます

Node.jsのnetモジュールは賢くて、パスの形式を見て自動判断します：

\\.\pipe\xxx → Windows Named Pipe
/tmp/xxx.sock → Unix Domain Socket

なのでNode.js版はOS固有のコードがなく、パス文字列を変えるだけで両対応できています。


# pip install websockets

from haglib_duplex import WebSocketDuplexServer, WebSocketDuplexClient, TypedPayload

# ===== サーバー =====
server = WebSocketDuplexServer()

async def on_recv(ch, msg):
    payload = msg.to_typed_payload()
    print(f"受信: {payload.get_text()}")
    
    response = TypedPayload().add_text("OK!").add_json('{"status":"success"}')
    await ch.reply(msg, response.to_message())

server.on_received = on_recv
await server.start(8080)

# ===== クライアント =====
client = WebSocketDuplexClient("ws://localhost:8080/")
await client.connect()

response = await client.send_and_receive_text("Hello!")
print(response.to_typed_payload().get_text())

await client.close()