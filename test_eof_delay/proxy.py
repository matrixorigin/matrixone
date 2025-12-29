#!/usr/bin/env python3
"""
模拟一个"坏"代理：客户端断开后，延迟一段时间才断开与后端的连接。
用于复现 MO 的 EOF 延迟问题。

使用方法：
1. 启动代理：python proxy.py --backend-host 127.0.0.1 --backend-port 6001 --listen-port 16001 --delay 60
2. mysql 客户端连接代理：mysql -h 127.0.0.1 -P 16001 -u root -p
3. 执行 LOAD DATA LOCAL，然后 Ctrl+C 或超时退出
4. 观察 MO 日志，EOF 会在 --delay 秒后才出现
"""

import socket
import threading
import argparse
import time
import sys

class DelayedProxy:
    def __init__(self, listen_port, backend_host, backend_port, delay_seconds):
        self.listen_port = listen_port
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.delay_seconds = delay_seconds
        
    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.listen_port))
        server.listen(5)
        print(f"[Proxy] Listening on port {self.listen_port}")
        print(f"[Proxy] Backend: {self.backend_host}:{self.backend_port}")
        print(f"[Proxy] Delay before closing backend connection: {self.delay_seconds}s")
        
        while True:
            client_sock, addr = server.accept()
            print(f"[Proxy] Client connected from {addr}")
            threading.Thread(target=self.handle_client, args=(client_sock, addr)).start()
    
    def handle_client(self, client_sock, client_addr):
        backend_sock = None
        try:
            # 连接后端
            backend_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            backend_sock.connect((self.backend_host, self.backend_port))
            print(f"[Proxy] Connected to backend")
            
            # 设置非阻塞用于转发
            client_sock.setblocking(False)
            backend_sock.setblocking(False)
            
            client_closed = False
            backend_closed = False
            
            while not (client_closed and backend_closed):
                # 从客户端读，转发到后端
                if not client_closed:
                    try:
                        data = client_sock.recv(65536)
                        if data:
                            backend_sock.sendall(data)
                        else:
                            # 客户端关闭了连接
                            print(f"[Proxy] Client closed connection, waiting {self.delay_seconds}s before closing backend...")
                            client_closed = True
                            # 关键：延迟关闭后端连接！
                            time.sleep(self.delay_seconds)
                            print(f"[Proxy] Now closing backend connection")
                            break
                    except BlockingIOError:
                        pass
                    except Exception as e:
                        print(f"[Proxy] Client read error: {e}")
                        client_closed = True
                
                # 从后端读，转发到客户端
                if not backend_closed:
                    try:
                        data = backend_sock.recv(65536)
                        if data:
                            if not client_closed:
                                client_sock.sendall(data)
                        else:
                            print(f"[Proxy] Backend closed connection")
                            backend_closed = True
                    except BlockingIOError:
                        pass
                    except Exception as e:
                        print(f"[Proxy] Backend read error: {e}")
                        backend_closed = True
                
                time.sleep(0.001)  # 避免 CPU 空转
                
        except Exception as e:
            print(f"[Proxy] Error: {e}")
        finally:
            if client_sock:
                try:
                    client_sock.close()
                except:
                    pass
            if backend_sock:
                try:
                    backend_sock.close()
                except:
                    pass
            print(f"[Proxy] Connection from {client_addr} closed")

def main():
    parser = argparse.ArgumentParser(description='Delayed proxy for testing EOF delay')
    parser.add_argument('--listen-port', type=int, default=16001, help='Port to listen on')
    parser.add_argument('--backend-host', type=str, default='127.0.0.1', help='Backend host')
    parser.add_argument('--backend-port', type=int, default=6001, help='Backend port')
    parser.add_argument('--delay', type=int, default=60, help='Delay in seconds before closing backend connection')
    
    args = parser.parse_args()
    
    proxy = DelayedProxy(args.listen_port, args.backend_host, args.backend_port, args.delay)
    proxy.start()

if __name__ == '__main__':
    main()
