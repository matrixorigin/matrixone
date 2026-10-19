#!/usr/bin/env python3
"""有界、公开 MySQL SQL ordered-FK 对照。需要 PyMySQL 1.1.2。

必须使用隔离服务/数据库；本驱动不启动或终止其它服务。每行 JSON 即时
flush，任何 SQL、oracle、wait barrier 或取消失败都导致非零退出。
"""
import argparse
import contextlib
import json
import os
import platform
import statistics
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pymysql

SHAPES = ("ordinary", "auto_pk", "auto_nonpk", "auto_composite", "self_composite")


def emit(value):
    print(json.dumps(value, ensure_ascii=False, sort_keys=True), flush=True)


def sql(conn, statement, args=None):
    with conn.cursor() as cur:
        cur.execute(statement, args)
        return cur.fetchall()


def scalar(conn, statement, args=None):
    return sql(conn, statement, args)[0][0]


def parent_rows(conn, table, count, self_ref=False):
    for offset in range(0, count, 256):
        rows = range(offset + 1, min(offset + 256, count) + 1)
        if self_ref:
            values = ",".join(f"({i},{2*i},{3*i},NULL,NULL)" for i in rows)
        else:
            values = ",".join(f"({i},{i},7,0)" for i in rows)
        sql(conn, f"insert into {table} values {values}")


def flush_parent(conn, database, table):
    result = sql(conn, "select mo_ctl('dn','flush',%s)", (f"{database}.{table}",))
    decoded = json.loads(result[0][0])
    statuses = decoded.get("result", [])
    if not statuses or any(item.get("returnStr") != "OK" for item in statuses):
        raise AssertionError(f"parent flush 未确认成功：{result}")


def prepare(conn, shape, parents):
    sql(conn, "drop table if exists fkbench_child")
    sql(conn, "drop table if exists fkbench_self")
    if shape == "self_composite":
        sql(conn, "create table fkbench_self (id bigint primary key, a bigint, b bigint, "
            "ca bigint, cb bigint, unique key uq(a,b), "
            "foreign key(ca,cb) references fkbench_self(a,b))")
        parent_rows(conn, "fkbench_self", parents, self_ref=True)
        return "fkbench_self"
    if shape == "ordinary":
        ddl = "id bigint primary key, ref bigint, foreign key(ref) references fkbench_parent(id)"
    elif shape == "auto_pk":
        ddl = "id bigint auto_increment primary key, payload bigint, foreign key(id) references fkbench_parent(id)"
    elif shape == "auto_nonpk":
        ddl = "id bigint auto_increment primary key, payload bigint, foreign key(id) references fkbench_parent(code)"
    else:
        ddl = "id bigint auto_increment primary key, scope bigint, foreign key(id,scope) references fkbench_parent(code,scope)"
    sql(conn, f"create table fkbench_child ({ddl})")
    return "fkbench_child"


def payload(shape, parents, children):
    if shape == "ordinary":
        return "id,ref", [f"{i},{i}" for i in range(1, children + 1)]
    if shape == "self_composite":
        return "id,a,b,ca,cb", [f"{parents+i},{2*(parents+i)},{3*(parents+i)},{2*i},{3*i}"
                               for i in range(1, children + 1)]
    name = "scope" if shape == "auto_composite" else "payload"
    value = 7 if name == "scope" else 1
    return name, [str(value)] * children


def make_statement(table, shape, parents, children, mode, data_dir):
    columns, values = payload(shape, parents, children)
    if mode == "insert":
        return f"insert ignore into {table} ({columns}) values " + ",".join(f"({v})" for v in values)
    path = data_dir / "input.csv"
    path.write_text("\n".join(values) + "\n")
    local = "local " if mode == "local" else ""
    # tempfile 由驱动创建，不接受外部 SQL 路径，路径仍使用 driver escape。
    return (f"load data {local}infile {pymysql.converters.escape_string(str(path))!r} "
            f"ignore into table {table} fields terminated by ',' lines terminated by '\\n' ({columns})")


def oracle(conn, table, shape, parents, children):
    where = f" where id>{parents}" if shape == "self_composite" else ""
    got = scalar(conn, f"select count(*) from {table}{where}")
    if got != children:
        raise AssertionError(f"接受行 oracle: {got} != {children}")
    if shape == "self_composite":
        missing = scalar(conn, "select count(*) from fkbench_self c left join fkbench_self p "
                         "on c.ca=p.a and c.cb=p.b where c.ca is not null and p.id is null")
    else:
        child = "c.ref" if shape == "ordinary" else "c.id"
        parent = "p.code" if shape in ("auto_nonpk", "auto_composite") else "p.id"
        extra = " and c.scope=p.scope" if shape == "auto_composite" else ""
        missing = scalar(conn, f"select count(*) from {table} c left join fkbench_parent p "
                         f"on {child}={parent}{extra} where p.id is null")
    if missing:
        raise AssertionError(f"FK oracle: {missing} 个孤儿")
    if shape.startswith("auto_"):
        bounds = sql(conn, f"select min(id),max(id),sum(id) from {table}")[0]
        if tuple(map(int, bounds)) != (1, children, children*(children+1)//2):
            raise AssertionError(f"最终 ID 映像错误：{bounds}")


def probe(args, operation, tables=(), ids=()):
    query = urllib.parse.urlencode({"tables": ",".join(tables), "ids": ",".join(map(str, ids))})
    url = f"{args.probe}/debug/claude-fkbench/{operation}?{query}"
    with urllib.request.urlopen(url, timeout=5) as response:
        body = response.read()
        return json.loads(body) if body else None


def trial(args, conn, shape, parents, children, mode, iteration, data_dir):
    table = prepare(conn, shape, parents)
    table_id = scalar(conn, "select rel_id from mo_catalog.mo_tables "
                      "where reldatabase=%s and relname=%s", (args.database, table))
    statement = make_statement(table, shape, parents, children, mode, data_dir)
    if shape == "self_composite" and args.storage == "persisted":
        flush_parent(conn, args.database, table)
    conn.begin()
    probe(args, "start", ("fkbench_parent", "fkbench_self"), (table_id,))
    try:
        started = time.perf_counter()
        sql(conn, statement)
        executed = time.perf_counter()
        conn.commit()
        committed = time.perf_counter()
    except BaseException:
        conn.rollback()
        raise
    finally:
        counters = probe(args, "stop")
    # 计量窗口在 oracle 前关闭；避免 oracle 的 join 读取污染 parent 扫描数据。
    oracle(conn, table, shape, parents, children)
    record = dict(kind="sample", label=args.label, revision=args.revision,
                  shape=shape, parents=parents, children=children, mode=mode, iteration=iteration,
                  execute_ms=1000*(executed-started), commit_ms=1000*(committed-executed),
                  execute_rows_per_second=children/(executed-started),
                  committed_rows_per_second=children/(committed-started), counters=counters)
    emit(record)
    return record


def contention(args, connect, admin, shape, action, iteration):
    parents, children = 4096, 16
    table = prepare(admin, shape, parents)
    parent_id = scalar(admin, "select rel_id from mo_catalog.mo_tables "
                       "where reldatabase=%s and relname='fkbench_parent'", (args.database,))
    holder, writer = connect(), connect()
    done = threading.Event()
    result = {}
    thread = None
    writer_id = scalar(writer, "select connection_id()")
    try:
        holder.begin()
        columns, values = payload(shape, parents, children)
        child_statement = f"insert ignore into {table} ({columns}) values " + ",".join(f"({v})" for v in values)
        parent_statement = f"update fkbench_parent set payload=payload+1 where id={parents}"
        if action == "cancel_child":
            sql(holder, parent_statement)
            competing_statement = child_statement
        else:
            sql(holder, child_statement)
            competing_statement = parent_statement

        def write():
            started = time.perf_counter()
            try:
                # 更新未被 child 引用的 parent 行；测出表级锁带来的额外串行化。
                sql(writer, competing_statement)
                result["writer_outcome"] = "committed"
            except Exception as exc:
                result["writer_outcome"] = "error"
                result["writer_error"] = str(exc)
            finally:
                result["writer_total_ms"] = 1000*(time.perf_counter()-started)
                done.set()

        thread = threading.Thread(target=write, daemon=True)
        thread.start()
        deadline = time.monotonic() + 5
        waiting = False
        while not done.is_set() and time.monotonic() < deadline:
            waiting = bool(scalar(admin, "select count(*) from mo_locks() l "
                                  "where l.table_id=%s and l.lock_wait<>''", (parent_id,)))
            if waiting:
                break
            # 轮询间隔不是锁等待的证明，只有公共锁视图的行才是 barrier。
            done.wait(0.01)
        if not waiting and not done.is_set():
            raise AssertionError("未观察到 writer 完成或真实锁等待 barrier")
        result.update(kind="contention", label=args.label, revision=args.revision,
                      shape=shape, action=action, iteration=iteration, observed_lock_wait=waiting)
        if waiting:
            triggered = time.perf_counter()
            if action in ("cancel", "cancel_child"):
                sql(admin, f"kill query {int(writer_id)}")
            else:
                holder.commit()
            if not done.wait(5):
                raise AssertionError("解除锁/取消后 writer 未在 5 秒内完成")
            result["trigger_to_completion_ms"] = 1000*(time.perf_counter()-triggered)
            if action in ("cancel", "cancel_child"):
                text = result.get("writer_error", "").lower()
                if result["writer_outcome"] != "error" or not any(w in text for w in ("cancel", "interrupt", "killed")):
                    raise AssertionError(f"未收到 query 取消错误：{result}")
            elif result["writer_outcome"] != "committed":
                raise AssertionError(f"holder commit 后 writer 失败：{result}")
        elif result.get("writer_outcome") != "committed":
            raise AssertionError(f"非阻塞 writer 失败：{result}")
        if action == "cancel_child" and waiting and scalar(admin, f"select count(*) from {table}") != 0:
            raise AssertionError("取消的 child 写入发生了提交")
        emit(result)
    finally:
        # 在关闭连接前显式释放 holder，避免失败路径遗留阻塞事务。
        with contextlib.suppress(Exception):
            holder.rollback()
        if thread and not done.is_set():
            with contextlib.suppress(Exception):
                sql(admin, f"kill query {int(writer_id)}")
            if not done.wait(5):
                with contextlib.suppress(Exception):
                    sql(admin, f"kill connection {int(writer_id)}")
        if thread:
            thread.join(timeout=5)
        with contextlib.suppress(Exception):
            holder.close()
        with contextlib.suppress(Exception):
            writer.close()
        if thread and thread.is_alive():
            raise AssertionError("writer 清理后仍存活")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--probe", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--repeats", type=int, default=3, choices=range(1, 11))
    parser.add_argument("--children", type=int, nargs="+", default=[16, 64, 256])
    parser.add_argument("--parents", type=int, nargs="+", default=[256, 4096])
    parser.add_argument("--modes", nargs="+", choices=("insert", "load", "local"), default=["insert"])
    parser.add_argument("--shapes", nargs="+", choices=SHAPES, default=SHAPES)
    parser.add_argument("--contention", action="store_true")
    parser.add_argument("--storage", choices=("memory", "persisted"), default="persisted")
    args = parser.parse_args()
    if not args.database.startswith("claude_fkbench_") or not args.database.replace("_", "").isalnum():
        parser.error("database 必须是专用 claude_fkbench_ 标识符")
    if any(n < 1 or n > 256 for n in args.children) or any(p < max(args.children) or p > 4096 for p in args.parents):
        parser.error("有界规模：1<=children<=256，children<=parents<=4096")
    cases = len(args.children)*len(args.parents)*len(args.shapes)*len(args.modes)*args.repeats
    if cases > 300:
        parser.error("单次执行最多 300 个计量窗口")

    def connect(database=True):
        conn = pymysql.connect(host="127.0.0.1", port=args.port, user="dump", password=os.environ.get("MO_PASSWORD", "111"),
                               database=args.database if database else None, autocommit=True,
                               local_infile=True, connect_timeout=5, read_timeout=20, write_timeout=20)
        return conn

    admin = connect(False)
    # 不复用/清空已有 database；create 失败就停止，避免覆盖其它任务的数据。
    created = False
    try:
        sql(admin, f"create database {args.database}")
        created = True
        sql(admin, f"use {args.database}")
        emit(dict(kind="environment", label=args.label, revision=args.revision,
                  platform=platform.platform(), cpu_count=os.cpu_count(), pymysql_version=pymysql.VERSION_STRING,
                  protocol="mysql", matrix=vars(args),
                  server_version=scalar(admin, "select version()"),
                  transaction_isolation=scalar(admin, "select @@transaction_isolation")))
        samples = []
        with tempfile.TemporaryDirectory(prefix="CLAUDE_fkbench_") as directory:
            for parents in args.parents:
                sql(admin, "drop table if exists fkbench_child")
                sql(admin, "drop table if exists fkbench_self")
                sql(admin, "drop table if exists fkbench_parent")
                sql(admin, "create table fkbench_parent (id bigint primary key, code bigint unique, "
                    "scope bigint, payload bigint, unique key uq(code,scope))")
                parent_rows(admin, "fkbench_parent", parents)
                if args.storage == "persisted":
                    flush_parent(admin, args.database, "fkbench_parent")
                for shape in args.shapes:
                    for children in args.children:
                        for mode in args.modes:
                            for iteration in range(args.repeats):
                                samples.append(trial(args, admin, shape, parents, children, mode, iteration, Path(directory)))
            if args.contention:
                # 公共父表必须有 4096 行，避免 --parents 自定义导致 writer 更新零行。
                sql(admin, "drop table if exists fkbench_child")
                sql(admin, "drop table if exists fkbench_self")
                sql(admin, "delete from fkbench_parent")
                parent_rows(admin, "fkbench_parent", 4096)
                for iteration in range(args.repeats):
                    for shape in ("ordinary", "auto_pk", "auto_nonpk", "auto_composite"):
                        for action in ("release", "cancel", "cancel_child"):
                            contention(args, connect, admin, shape, action, iteration)
        for key in sorted({(s["shape"], s["parents"], s["children"], s["mode"]) for s in samples}):
            group = [s for s in samples if (s["shape"], s["parents"], s["children"], s["mode"]) == key]
            latencies = [s["execute_ms"] for s in group]
            emit(dict(kind="summary", label=args.label, shape=key[0], parents=key[1], children=key[2], mode=key[3],
                      samples=len(group), execute_ms_min=min(latencies), execute_ms_median=statistics.median(latencies),
                      execute_ms_max=max(latencies)))
    finally:
        try:
            if created:
                with contextlib.suppress(Exception):
                    admin.rollback()
                sql(admin, f"drop database {args.database}")
                if scalar(admin, "select count(*) from mo_catalog.mo_database where datname=%s", (args.database,)):
                    raise AssertionError("benchmark database 清理失败")
                emit(dict(kind="cleanup", label=args.label, database_absent=True))
        finally:
            admin.close()


if __name__ == "__main__":
    main()
