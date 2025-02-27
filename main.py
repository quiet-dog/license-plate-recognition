import cv2
import time
import os
import hyperlpr3 as lpr3
import asyncio
import websockets
import json
from collections import defaultdict
from typing import Dict, Set
import mysql.connector
import uuid
from datetime import datetime, timedelta


# rtsp_url = "rtsp://admin:147258369q@192.168.5.9:554/Streaming/Channels/102"

# # 输出文件夹（保存 JPG 文件的地方）
# output_dir = "frames"
# if not os.path.exists(output_dir):
#     os.makedirs(output_dir)

# # 打开 RTSP 流
# cap = cv2.VideoCapture(rtsp_url)

# # 检查是否成功打开
# if not cap.isOpened():
#     print("Error: 无法打开 RTSP 流")
#     exit()

# # 帧计数器
# frame_count = 0
# # 每隔几帧保存一次（例如每 30 帧保存一张，避免保存过多）
# save_interval = 30
# catcher = lpr3.LicensePlateCatcher()

# print("开始读取 RTSP 流，按 'q' 退出...")

# while True:
#     # 读取一帧
#     ret, frame = cap.read()
    
#     # 如果读取失败，退出循环
#     if not ret:
#         print("Error: 无法读取帧，可能是流中断")
#         break
    
#     # 每隔 save_interval 帧保存一次
#     ret, jpg_data = cv2.imencode('.jpg', frame)
    
#     if ret:
#         # jpg_data 是一个字节数组，包含 JPG 编码后的数据
#         print(f"帧已转为 JPG，字节长度: {len(jpg_data)}")
#         print(catcher(frame))
#         # 这里可以进一步处理 jpg_data，例如发送到网络、存到内存等
#     else:
#         print("Error: 帧转 JPG 失败")
    
#     # 显示帧（可选，方便调试）
#     # cv2.imshow("RTSP Stream", frame)
    
#     # 帧计数器递增
#     frame_count += 1
    
#     # 按 'q' 退出
#     if cv2.waitKey(1) & 0xFF == ord('q'):
#         break

# # 释放资源
# cap.release()
# # cv2.destroyAllWindows()
# print("程序结束")

connection = mysql.connector.connect(
    host="localhost",       # 数据库主机
    user="root",   # 数据库用户名
    password="123456",  # 数据库密码
    database="carpai",   # 要连接的数据库
    port="9001"
)

cursor = connection.cursor()

# 执行查询




# websocket客户端
class WebSocketServer:
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.subscribers: Set[websockets.WebSocketServerProtocol] = set()

    # 确保这里有 path 参数
    async def handle_client(self, websocket: websockets.WebSocketServerProtocol, path: str):
        # print(f"新客户端连接: {id(websocket)}, 路径: {path}")
        self.subscribers.add(websocket)
        try:
            async for message in websocket:
                print(f"收到客户端消息: {message}")
                await websocket.send(json.dumps({"echo": message}))
        except websockets.ConnectionClosed:
            print(f"客户端 {id(websocket)} 断开连接")
            self.subscribers.remove(websocket)

    async def send_message_to_subscribers(self, message: dict):
        if not self.subscribers:
            print("没有订阅者")
            return
        message_json = json.dumps(message)
        tasks = [ws.send(message_json) for ws in self.subscribers]
        await asyncio.gather(*tasks)
        print(f"已向 {len(self.subscribers)} 个订阅者发送消息: {message_json}")
    
    async def run(self):
        server = await websockets.serve(self.handle_client, self.host, self.port)
        print(f"WebSocket 服务器运行在 ws://{self.host}:{self.port}")
        await server.wait_closed()

class Stream:
    def __init__(self, rtsp_url: str):
        self.rtsp_url = rtsp_url
        self.video_capture = None
        self.is_opened = False

    def open(self):
        self.video_capture = cv2.VideoCapture(self.rtsp_url)
        self.is_opened = self.video_capture.isOpened()
        return self.is_opened

    def read(self):
        if self.is_opened and self.video_capture:
            return self.video_capture.read()
        return False, None

    def release(self):
        if self.video_capture:
            self.video_capture.release()
        self.is_opened = False

class StreamManager:
    def __init__(self, ws: WebSocketServer):
        self.catcher = lpr3.LicensePlateCatcher()
        self.streams: Dict[str, Stream] = {}
        self.ws = ws

    async def add_stream(self, stream_id: str, rtsp_url: str):
        if stream_id in self.streams:
            print(f"流 {stream_id} 已存在")
            return False

        stream = Stream(rtsp_url)
        if not stream.open():
            print(f"Error: 无法打开 RTSP 流 {rtsp_url}")
            return False

        self.streams[stream_id] = stream
        asyncio.create_task(self.run_stream(stream_id))
        return True

    async def run_stream(self, stream_id: str):
        while stream_id in self.streams:
            stream = self.streams[stream_id]
            print(f"开始读取 RTSP 流 {stream.rtsp_url}")

            while stream.is_opened:
                ret, frame = stream.read()
                if not ret:
                    print(f"Error: 无法读取帧，流 {stream.rtsp_url} 中断")
                    stream.release()
                    break
                # 读取本地的jpg图片
                frame = cv2.imread("/home/zks/Documents/20250226095812158.jpg")                

                result = self.catcher(frame)
                print("识别结果result",result)
                if result:
                    # 获取result的第一个数据
                    result = result[0]
                    # 获取conf,plate_type,code
                    # conf, plate_type, code = result
                    code = result[0]
                    plate_type = result[2]
                    conf = result[1]
                    now = datetime.now()
                    time_ago = (now - timedelta(seconds=5)).strftime('%Y-%m-%d %H:%M:%S')
                    now = now.strftime('%Y-%m-%d %H:%M:%S')
                    print(now,time_ago)
                    # 如果conf大于0.95
                    if conf > 0.95:
                        # 查询数据库相差3s是否有相同的车牌号
                        query = f"SELECT * FROM car_log_models WHERE car_num = '{code}' AND created_at BETWEEN '{time_ago}' AND '{now}' AND device_id = {int(stream_id)}"
                        cursor.execute(query)
                        result = cursor.fetchall()
                        print("查询结果",query)
                        print("相近的结果",result)
                        # 将图片保存到本地
                        # 生成随机uuid
                        uri = str(uuid.uuid1())
                        cv2.imwrite(f"/home/zks/Documents/{uri}.jpg", frame)
                        # 如果查询结果为空
                        if not result:
                            # 插入数据
                            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                            query = f"INSERT INTO car_log_models (car_num,plate_type,device_id,created_at,updated_at,uri) VALUES ('{code}','{plate_type}',{int(stream_id)},'{current_time}','{current_time}','{uri}')"
                            cursor.execute(query)
                            connection.commit()
                            # 发送消息
                            message = {
                                "stream_id": stream_id,
                                "license_plate": code,
                                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                            }
                            await self.ws.send_message_to_subscribers(message)

                    # message = {
                    #     "stream_id": stream_id,
                    #     "license_plate": result,
                    #     "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    # }
                    # await self.ws.send_message_to_subscribers(message)

                await asyncio.sleep(1)

            print(f"流 {stream_id} 断开，将在 5 秒后尝试重连")
            await asyncio.sleep(5)
            if stream_id in self.streams and not stream.open():
                print(f"Error: 重连失败 {stream.rtsp_url}")

     # 运行一个线程每5s执行这个任务
   
    async def get_mysql_data(self):
        while True:
            ids = []
        # 便利self.streams,获取流id,生成一个数组
            for stream in self.streams:
                ids.append(stream)
            # 执行查询
            placeholders = ','.join(['%s'] * len(ids))
            query = f"SELECT * FROM device_models WHERE deleted_at IS NULL AND id NOT IN ({placeholders})"
            print("ids",ids)
            # 判断ids是否为空
            if not ids:
                query = "SELECT * FROM device_models WHERE deleted_at IS NULL"
            
            # 获取查询结果
            cursor.execute(query, tuple(ids))
            result = cursor.fetchall()
            # 如果查询结果不为空
            if result:
                # 遍历查询结果
                for row in result:
                    # 获取查询结果的id
                    id = row[0]
                    # 获取查询结果的rtsp_url
                    rtsp_url = row[13]
                    # 添加流
                    await self.add_stream(str(id), rtsp_url)

            await asyncio.sleep(5)

async def main():
    ws_server = WebSocketServer()
    stream_manager = StreamManager(ws_server)
    # await stream_manager.add_stream("1", "rtsp://admin:147258369q@192.168.5.9:554/Streaming/Channels/102")
    # await stream_manager.get_mysql_data()
    # 异步
    asyncio.create_task(stream_manager.get_mysql_data())
    await ws_server.run()
    # 程序结束时释放资源,关闭数据库连接
    cursor.close()
    connection.close()

if __name__ == "__main__":
    asyncio.run(main())