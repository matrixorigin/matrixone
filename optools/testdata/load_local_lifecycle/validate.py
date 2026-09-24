# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by law or agreed to in writing, software is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import time
import pymysql
from pymysql.constants import COMMAND

def connect(port=16001):
    return pymysql.connect(host='127.0.0.1', port=port, user=os.getenv('MO_TEST_USER', 'root'), password=os.getenv('MO_TEST_PASSWORD', '111'),
                           autocommit=True, local_infile=True, read_timeout=10, write_timeout=10)

def sql(c, statement):
    with c.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()

def upload(c):
    c._execute_command(COMMAND.COM_QUERY,
        "load data local infile 'client.csv' into table pr26109_probe.uploads fields terminated by ',' lines terminated by '\\n'")
    packet = c._read_packet()
    assert packet.is_load_local_packet(), packet.get_all_data()

deadline = time.monotonic() + 120
while True:
    try:
        admin = connect()
        other = connect(16002)
        break
    except Exception:
        if time.monotonic() > deadline:
            raise
        time.sleep(1)

try:
    sql(admin, 'drop database if exists pr26109_probe')
    sql(admin, 'create database pr26109_probe')
    sql(admin, 'create table pr26109_probe.uploads (id int primary key)')
    sql(admin, 'create table pr26109_probe.txn_rows (id int primary key)')
    sql(admin, 'insert into pr26109_probe.txn_rows values (1),(2)')
    for port in (16001,16002):
        c = connect(port)
        try:
            sql(c, 'begin')
            sql(c, 'insert into pr26109_probe.txn_rows values (3)')
            got = sql(c, 'select count(*),sum(id) from pr26109_probe.txn_rows')
            assert got == ((3,6),), got
            peer = other if port == 16001 else admin
            assert sql(peer, 'select count(*),sum(id) from pr26109_probe.txn_rows') == ((2,3),)
            sql(c, 'rollback')
            assert sql(c, 'select count(*),sum(id) from pr26109_probe.txn_rows') == ((2,3),)
            print('PASS read-your-writes/isolation/rollback', port, flush=True)
            upload(c)
            c.write_packet(b'1\n2\n')
            c.write_packet(b'')
            response = c._read_packet()
            assert response.is_ok_packet(), response.get_all_data()
            assert sql(c, 'select sum(id) from pr26109_probe.uploads') == ((3,),)
            sql(c, 'truncate table pr26109_probe.uploads')
            print('PASS LOCAL success and connection reuse', port, flush=True)
        finally:
            c.close()

    for mode in ('idle','partial_header','partial_payload','continuous_packets','udf_import'):
        c = connect()
        try:
            if mode == 'udf_import':
                c._execute_command(COMMAND.COM_QUERY,
                    "create function pr26109_probe.udf_add(x int) returns int language python import 'probe.py' handler 'add'")
                request = c._read_packet()
                assert request.is_load_local_packet(), request.get_all_data()
            else:
                upload(c)
            if mode == 'partial_header':
                c._write_bytes(b'\x04\x00')
            elif mode == 'partial_payload':
                c._write_bytes(bytes((4,0,0,c._next_seq_id)) + b'1')
            elif mode == 'continuous_packets':
                c.write_packet(b'1\n')
            start = time.monotonic()
            sql(admin, 'kill query %d' % c.thread_id())
            # No protocol EOF is sent. The server must close the connection,
            # including when a peer continues writing valid data packets.
            if mode == 'continuous_packets':
                try:
                    for _ in range(100):
                        c.write_packet(b'2\n')
                except (OSError, pymysql.Error):
                    pass
            if c._sock is not None:
                try:
                    payload = c._sock.recv(1)
                    assert payload == b'', payload
                except ConnectionResetError:
                    pass
            elapsed = time.monotonic() - start
            assert elapsed < 10, elapsed
            assert sql(admin, 'select count(*) from pr26109_probe.uploads') == ((0,),)
            if mode == 'udf_import':
                assert sql(admin, "select count(*) from mo_catalog.mo_user_defined_function where db='pr26109_probe' and name='udf_add'") == ((0,),)
            print('PASS LOCAL cancel/no EOF/rollback', mode, round(elapsed,4), flush=True)
        finally:
            c.close()
finally:
    sql(admin, 'drop database if exists pr26109_probe')
    assert sql(admin, "show databases like 'pr26109_probe'") == ()
    admin.close()
    other.close()
print('PASS teardown', flush=True)
