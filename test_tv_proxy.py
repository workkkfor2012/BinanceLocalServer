import asyncio
import websockets
import json

async def test_tv_proxy():
    uri = "ws://localhost:6001"
    async with websockets.connect(uri) as websocket:
        print(f"Connected to {uri}")
        
        # 发送订阅请求
        sub_request = [
            "addSubscriptions",
            {"symbols": ["CAPITALCOM:US500", "FX:EURUSD"]}
        ]
        await websocket.send(json.dumps(sub_request))
        print(f"Sent: {sub_request}")
        
        # 接收并打印消息
        try:
            while True:
                response = await websocket.recv()
                data = json.loads(response)
                msg_type = data.get("type")
                payload = data.get("payload", {})
                symbol = payload.get("symbol", "unknown")
                
                if msg_type == "history":
                    print(f"\n[History] {symbol}: Received {len(payload.get('data', []))} bars")
                elif msg_type == "updateLast":
                    kline = payload.get("kline", {})
                    print(f"[Update] {symbol}: Price={kline.get('close')} Time={kline.get('timestamp')}")
                else:
                    print(f"[Other] {response}")
        except websockets.exceptions.ConnectionClosed:
            print("Connection closed")

if __name__ == "__main__":
    asyncio.run(test_tv_proxy())
