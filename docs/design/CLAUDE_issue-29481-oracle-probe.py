#!/usr/bin/env python3
"""C02 定向参考探测：同一 send buffer 记录请求，离线检查 JSON 往返。"""

import argparse
import json
import socket
import struct
from pathlib import Path

# 原始字符串使 SQL 的引号、美元号和单个反斜杠不经过 shell 重构。
STAGED_SQL = (
    r"SET NAMES utf8mb4; SET character_set_connection=ascii,sql_mode='NO_BACKSLASH_ESCAPES'; "
    r"SELECT HEX('a\nb'); BAD SQL; SELECT 1"
).encode("ascii")
STATE_SQL = (
    b"SELECT @@character_set_client,@@character_set_connection,"
    b"@@character_set_results,@@sql_mode"
)
PARTIAL_SQL = (
    b"SET NAMES utf8mb4; SELECT JSON_EXTRACT(x,'$') "
    b"FROM (SELECT '1' x UNION ALL SELECT 'bad') t"
)
FOLLOWUP_SQL = b"SELECT 1"
CAPABILITIES = 1 | 512 | 8192 | 32768 | 65536 | 131072 | 524288
DEFAULT_MODE = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)


def recv_exact(conn, size):
    data = bytearray()
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            raise EOFError("参考连接在 packet 中途关闭")
        data.extend(chunk)
    return bytes(data)


def recv_payload(conn):
    header = recv_exact(conn, 4)
    return recv_exact(conn, int.from_bytes(header[:3], "little"))


def send_payload(conn, payload, sequence=0):
    conn.sendall(len(payload).to_bytes(3, "little") + bytes([sequence]) + payload)


def read_lenenc(data, offset):
    length = data[offset]
    offset += 1
    if length == 251:
        return None, offset
    if length >= 252:
        width = {252: 2, 253: 3, 254: 8}[length]
        length = int.from_bytes(data[offset:offset + width], "little")
        offset += width
    end = offset + length
    if end > len(data):
        raise ValueError("截断的 lenenc 字段")
    return data[offset:end], end


def parse_error(data):
    if data[3:4] != b"#":
        raise ValueError("缺少 Protocol 4.1 SQLSTATE")
    return {
        "error": int.from_bytes(data[1:3], "little"),
        "state": data[4:9].decode("ascii"),
        "message_hex": data[9:].hex(),
    }


def query(conn, trace, sql):
    payload = b"\x03" + sql
    # 唯一记录来源是即将发送的 buffer，不从另一份 SQL 表重构。
    record = {"trace": trace, "query_hex": payload[1:].hex(), "result": []}
    send_payload(conn, payload)
    packets = []

    def read():
        data = recv_payload(conn)
        packets.append(data.hex())
        return data

    while True:
        data = read()
        if data[0] == 255:
            record["result"].append(parse_error(data))
            break
        if data[0] == 0:
            # 此小探测的 SET 均是 affected_rows=last_insert_id=0。
            if data[1:3] != b"\x00\x00":
                raise ValueError("非预期 SET OK packet")
            record["result"].append({"ok_hex": data.hex()})
            status = int.from_bytes(data[3:5], "little")
        else:
            count = data[0]  # 此小探测只有 1 或 4 列。
            if count not in (1, 4):
                raise ValueError("非预期列数")
            columns = []
            for _ in range(count):
                column = read()
                offset = 0
                names = []
                for _ in range(6):
                    name, offset = read_lenenc(column, offset)
                    names.append(name.hex())
                if column[offset] != 12:
                    raise ValueError("非预期列定义长度")
                offset += 1
                columns.append({
                    "names_hex": names,
                    "charset": int.from_bytes(column[offset:offset + 2], "little"),
                    "length": int.from_bytes(column[offset + 2:offset + 6], "little"),
                    "type": column[offset + 6],
                })
            if read()[0] != 254:
                raise ValueError("缺少列定义 EOF")
            result = {"columns": columns, "rows": []}
            record["result"].append(result)
            while True:
                data = read()
                if data[0] == 255:
                    result.update(parse_error(data))
                    status = 0
                    break
                if data[0] == 254 and len(data) < 9:
                    status = int.from_bytes(data[3:5], "little")
                    result["status"] = status
                    break
                offset = 0
                row = []
                for _ in range(count):
                    value, offset = read_lenenc(data, offset)
                    row.append(None if value is None else value.hex())
                if offset != len(data):
                    raise ValueError("行 packet 仍有未解码字节")
                result["rows"].append(row)
        if not status & 8:
            break
    record["response_payloads_hex"] = packets
    assert bytes.fromhex(record["query_hex"]) == payload[1:]
    return record


def capture(host, port, user):
    conn = socket.create_connection((host, port), timeout=5)
    try:
        recv_payload(conn)
        # 仅支持本地无密码参考账户；不探测或记录用户密码。
        login = struct.pack("<IIB", CAPABILITIES, 16777216, 45) + bytes(23)
        login += user.encode("ascii") + b"\0\0mysql_native_password\0"
        send_payload(conn, login, 1)
        reply = recv_payload(conn)
        if reply[0] == 254:
            send_payload(conn, b"", 3)
            reply = recv_payload(conn)
        if reply[:2] == b"\x01\x03":
            reply = recv_payload(conn)
        if reply[0] != 0:
            raise RuntimeError("参考账户必须支持本地无密码认证")
        provenance = query(conn, "reference server", b"SELECT VERSION(),@@sql_mode,@@max_allowed_packet,@@character_set_server")
        row = provenance["result"][0]["rows"][0]
        assert row == [b"8.4.11".hex(), DEFAULT_MODE.encode().hex(), b"67108864".hex(), b"utf8mb4".hex()]
        records = [
            query(conn, "mixed SET and invalid later", STAGED_SQL),
            query(conn, "SET invalid later keeps state", STATE_SQL),
        ]
        send_payload(conn, b"\x1f")
        reset = recv_payload(conn)
        assert reset[0] == 0
        records.append({"command_hex": "1f", "reset_ok_hex": reset.hex()})
        records.append(query(conn, "after reset", STATE_SQL))
        records.append(query(conn, "partial result failure", PARTIAL_SQL))
        # 不插入其它命令：此成功响应直接证明上一 ERR 后的同连接复用。
        records.append(query(conn, "successful request immediately after partial error", FOLLOWUP_SQL))
        check(records)
        return {"provenance": provenance, "records": records}
    finally:
        conn.close()


def check_request(record, expected):
    if bytes.fromhex(record["query_hex"]) != expected:
        raise AssertionError(f"{record['trace']}: 保存的请求不是实际指定的 SQL")


def check(records):
    """不需要服务器：同时检查确切 SQL、响应与序列，而非仅 JSON/hex 合法。"""
    by_trace = {record["trace"]: record for record in records if "trace" in record}
    expected = {
        "mixed SET and invalid later": STAGED_SQL,
        "SET invalid later keeps state": STATE_SQL,
        "after reset": STATE_SQL,
        "partial result failure": PARTIAL_SQL,
        "successful request immediately after partial error": FOLLOWUP_SQL,
    }
    for trace, sql in expected.items():
        check_request(by_trace[trace], sql)
    staged = by_trace["mixed SET and invalid later"]["result"]
    assert len(staged) == 4
    assert staged[2]["columns"][0]["names_hex"][4] == br"HEX('a\nb')".hex()
    assert staged[2]["rows"] == [[b"615C6E62".hex()]]
    assert (staged[3]["error"], staged[3]["state"]) == (1064, "42000")
    assert by_trace["SET invalid later keeps state"]["result"][0]["rows"] == [[
        b"utf8mb4".hex(), b"ascii".hex(), b"utf8mb4".hex(), b"NO_BACKSLASH_ESCAPES".hex(),
    ]]
    assert by_trace["after reset"]["result"][0]["rows"] == [[
        b"utf8mb4".hex(), b"utf8mb4".hex(), b"utf8mb4".hex(), DEFAULT_MODE.encode().hex(),
    ]]
    partial = by_trace["partial result failure"]["result"][1]
    assert partial["columns"][0]["names_hex"][4] == b"JSON_EXTRACT(x,'$')".hex()
    assert partial["rows"] == [["31"]]
    assert (partial["error"], partial["state"]) == (3141, "22032")
    assert "status" not in partial
    packets = by_trace["partial result failure"]["response_payloads_hex"]
    assert packets[-2] == "0131"
    assert bytes.fromhex(packets[-1])[:9] == b"\xff\x45\x0c#22032"
    followup = by_trace["successful request immediately after partial error"]
    assert followup["result"][0]["rows"] == [["31"]]
    assert followup["response_payloads_hex"][-2:] == ["0131", "fe00000200"]
    index = next(i for i, record in enumerate(records) if record.get("trace") == "partial result failure")
    assert records[index + 1] is followup
    # 这是往返格式控制：SQL 的引号、美元号、单反斜杠必须原封不动。
    restored = json.loads(json.dumps(records))
    assert restored == records
    # 旧损坏记录不能通过：不是只要合法 hex 就算证据。
    for trace, sql in (("mixed SET and invalid later", b"SELECT HEX(a\nb)"),
                       ("partial result failure", b"SELECT JSON_EXTRACT(x,) FROM (SELECT 1 x UNION ALL SELECT bad) t")):
        bad = dict(by_trace[trace], query_hex=sql.hex())
        try:
            check_request(bad, expected[trace])
        except AssertionError:
            pass
        else:
            raise AssertionError("损坏的 SQL 被错误接受")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--capture", type=Path, help="在已固定本地 MySQL 上捕获到新文件")
    mode.add_argument("--check", type=Path, help="离线检查完整 oracle 或 capture 文件")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    args = parser.parse_args()
    if args.capture:
        # 不覆盖现有证据；全部检查成功后才创建输出文件。
        if args.capture.exists():
            parser.error("capture 文件已存在，请使用新路径")
        data = capture(args.host, args.port, args.user)
        with args.capture.open("x") as output:
            json.dump(data, output, indent=2)
            output.write("\n")
    else:
        data = json.loads(args.check.read_text())
        check(data.get("records", data.get("prepared_and_lifecycle_cases", [])))
    print("PASS: 确切 SQL/响应、state/reset、部分结果后同连接成功请求与 JSON 往返")


if __name__ == "__main__":
    main()
