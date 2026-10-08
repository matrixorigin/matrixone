#!/usr/bin/env python3
"""补采 codec 畸形序列 oracle；保留实际请求与收到的 response，不覆盖文件。"""
import argparse
import importlib.util
import json
import socket
import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
SPEC = importlib.util.spec_from_file_location(
    "oracle", ROOT / "docs/design/CLAUDE_issue-29481-oracle-probe.py"
)
P = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(P)
INPUTS = (
    "ff", "ff41", "ff4142", "ff414243", "f5414243", "f8414243",
    "c3", "c341", "e2", "e241", "e24142", "f0", "f041", "f04142", "f0414243",
    "80", "80414243", "41ff42", "41c342", "41e28242", "41eda08042",
    "41f490808042", "418042", "41c08042", "c3a9f09f9880", "7f80ff", "00",
)
# 枚举来自 metadata.go / encoding.go，不使用第二份名称准入表。
BOUNDARIES = (
    ("connection", 4, 2, 1, b"SET NAMES utf8mb4; SET character_set_connection=ascii"),
    ("result", 4, 2, 2, b"SET NAMES utf8mb4; SET character_set_results=ascii"),
    ("convert", 1, 4, 3, b"SET NAMES utf8mb4"),
    ("ascii-source", 2, 4, 1, b"SET NAMES utf8mb4; SET character_set_client=ascii"),
    ("convert-utf8-ascii", 4, 2, 3, b"SET NAMES utf8mb4"),
    ("convert-ascii-utf8", 2, 4, 3, b"SET NAMES utf8mb4"),
)


def capture():
    conn = socket.create_connection(("127.0.0.1", 3306), timeout=5)
    try:
        P.recv_payload(conn)
        login = struct.pack("<IIB", P.CAPABILITIES, 16777216, 45) + bytes(23)
        P.send_payload(conn, login + b"root\0\0mysql_native_password\0", 1)
        reply = P.recv_payload(conn)
        if reply[0] == 254:
            P.send_payload(conn, b"", 3)
            reply = P.recv_payload(conn)
        if reply[:2] == b"\x01\x03":
            reply = P.recv_payload(conn)
        if reply[0] != 0:
            raise RuntimeError("需要本地无密码参考账户")
        server = P.query(conn, "server", b"SELECT VERSION()")
        version = bytes.fromhex(server["result"][0]["rows"][0][0]).decode("ascii")
        if version != "8.4.11":
            raise RuntimeError(f"参考版本变更: {version}")
        cases = []
        for raw in INPUTS:
            for name, src, dst, policy, setting in BOUNDARIES:
                state = P.query(conn, "set", setting)
                if any("error" in item for item in state["result"]):
                    raise RuntimeError("参考会话设置失败")
                if name == "convert":
                    sql = b"SELECT CONVERT(_binary x'" + raw.encode("ascii") + b"' USING utf8mb4)"
                elif name == "convert-utf8-ascii":
                    sql = b"SELECT CONVERT(_utf8mb4'" + bytes.fromhex(raw) + b"' USING ascii)"
                elif name == "convert-ascii-utf8":
                    sql = b"SELECT CONVERT(_ascii'" + bytes.fromhex(raw) + b"' USING utf8mb4)"
                else:
                    sql = b"SELECT '" + bytes.fromhex(raw) + b"'"
                result = P.query(conn, name + ":" + raw, sql)
                if "rows" not in result["result"][0]:
                    raise RuntimeError(f"{result['trace']}: {result['result']}")
                cases.append({
                    "name": result["trace"], "src": src, "dst": dst, "policy": policy,
                    "input_hex": raw, "output_hex": result["result"][0]["rows"][0][0],
                    "setup_query_hex": state["query_hex"], "query_hex": result["query_hex"],
                    "response_payloads_hex": result["response_payloads_hex"],
                })
        return {"version": version, "server": server, "cases": cases}
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    args = parser.parse_args()
    if args.capture.exists():
        raise FileExistsError(args.capture)
    data = capture()
    with args.capture.open("x") as output:
        # 每个样本一行，原始 packet 和对应请求相邻，避免重复 SET metadata。
        output.write('{\n  "version": ' + json.dumps(data["version"]) + ',\n')
        output.write('  "server": ' + json.dumps(data["server"]) + ',\n  "cases": [\n')
        for index, case in enumerate(data["cases"]):
            output.write("    " + json.dumps(case) + ("," if index + 1 < len(data["cases"]) else "") + "\n")
        output.write("  ]\n}\n")
    print(f"已记录 {len(data['cases'])} 个真实 wire 样本: {args.capture}")
