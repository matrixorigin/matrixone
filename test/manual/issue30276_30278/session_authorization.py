#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Public MySQL regression; requires PyMySQL and two CNs behind a proxy."""
import argparse
import concurrent.futures
import contextlib
import json
import struct
import time

import pymysql
from pymysql import _auth

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--proxy-port', type=int, default=6001)
parser.add_argument('--cn-ports', type=int, nargs=2, required=True)
parser.add_argument('--password', default='111')
args = parser.parse_args()
USER = 'session_auth_low'
ROLE = 'session_auth_reader'
DB = 'session_auth_regression'


def connect(port=None, user='root', multi=False):
    return pymysql.connect(host='127.0.0.1', port=port or args.proxy_port,
                           user=user, password=args.password, autocommit=True,
                           read_timeout=15, write_timeout=10,
                           client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS if multi else 0)


def sql(conn, statement, params=None):
    with conn.cursor() as cursor:
        cursor.execute(statement, params)
        return cursor.fetchall()


def rejected(conn, statement):
    try:
        sql(conn, statement)
    except pymysql.MySQLError as error:
        assert 'access denied' in str(error).lower(), str(error)
        return
    raise AssertionError(f'unauthorized command succeeded: {statement}')


def change_user(conn, user, password):
    auth = _auth.scramble_native_password(password.encode(), conn.salt)
    payload = (user.encode() + b'\0' + bytes([len(auth)]) + auth + b'\0' +
               struct.pack('<H', pymysql.charset.charset_by_name(conn.charset).id) + b'mysql_native_password\0')
    conn._execute_command(0x11, payload)  # COM_CHANGE_USER on the existing tunnel
    packet = conn._read_packet()
    if packet.is_auth_switch_request():
        packet.read_uint8()
        plugin = packet.read_string()
        assert plugin == b'mysql_native_password', plugin
        salt = packet.read_all().rstrip(b'\0')
        conn.write_packet(_auth.scramble_native_password(password.encode(), salt))
        packet = conn._read_packet()
    assert packet.is_ok_packet(), 'COM_CHANGE_USER did not return OK'


def node(admin, conn):
    cid = sql(conn, 'select connection_id()')[0][0]
    rows = sql(admin, 'select node_id from information_schema.processlist where conn_id=%s', (cid,))
    assert len(rows) == 1, rows
    return rows[0][0]


def running(admin, cid, marker):
    return sql(admin, 'select conn_id from information_schema.processlist where conn_id=%s and info like %s',
               (cid, '%' + marker + '%'))


@contextlib.contextmanager
def request(admin, target, label):
    cid = sql(target, 'select connection_id()')[0][0]
    marker = 'session_auth_' + label
    def run():
        try:
            return ('completed', sql(target, 'select sleep(60) /* ' + marker + ' */'))
        except pymysql.MySQLError as error:
            return ('canceled', str(error))
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        future = pool.submit(run)
        try:
            deadline = time.monotonic() + 10
            while not running(admin, cid, marker):
                assert not future.done(), future.result()
                assert time.monotonic() < deadline, 'target never entered processlist'
                time.sleep(.02)  # Poll an observable server phase, never schedule a race.
            yield cid, future, marker
        finally:
            if not future.done():
                sql(admin, f'kill query {cid}')
            result = future.result(timeout=10)
            assert result[0] == 'canceled', result


def visibility(conn, other_id):
    for statement in [
        'select conn_id from information_schema.processlist where conn_id=%s',
        'select conn_id from processlist() p where conn_id=%s',
    ]:
        assert not sql(conn, statement, (other_id,)), statement
    for statement in ['show processlist', 'show full processlist']:
        rows = sql(conn, statement)
        assert all(row[3] == 'sys' and row[4] == USER for row in rows), rows


def main():
    with contextlib.ExitStack() as stack:
        admin = stack.enter_context(contextlib.closing(connect()))
        try:
            for statement in [f'drop user if exists {USER}', f'drop role if exists {ROLE}',
                              f'drop database if exists {DB}', f'create database {DB}',
                              f'create table {DB}.t(a int)', f'create role {ROLE}',
                              f'grant connect on account * to {ROLE}',
                              f'grant select on table {DB}.t to {ROLE}',
                              f"create user {USER} identified by '{args.password}' default role {ROLE}",
                              f'grant {ROLE} to root']:
                sql(admin, statement)
            actor = stack.enter_context(contextlib.closing(connect(user=USER)))
            sql(actor, f'set role {ROLE}')
            cns = [stack.enter_context(contextlib.closing(connect(port))) for port in args.cn_ports]
            actor_node = node(admin, actor)
            targets = {node(admin, conn): port for conn, port in zip(cns, args.cn_ports)}
            remote_port = next(port for name, port in targets.items() if name != actor_node)
            target = stack.enter_context(contextlib.closing(connect(remote_port)))
            assert node(admin, target) != actor_node
            with request(admin, target, 'root') as (cid, future, marker):
                visibility(actor, cid)
                sql(actor, "prepare pl from 'select conn_id from information_schema.processlist where conn_id=" + str(cid) + "'")
                assert not sql(actor, 'execute pl')
                sql(actor, 'deallocate prepare pl')
                rejected(actor, f'kill query {cid}')
                rejected(actor, f'kill connection {cid}')
                assert not future.done() and running(admin, cid, marker)
                sql(admin, f'kill query {cid}')
            assert sql(target, 'select 1') == ((1,),)
            # Active-role downgrade and upgrade use the existing proxy backend.
            role_actor = stack.enter_context(contextlib.closing(connect(multi=True)))
            low_target = stack.enter_context(contextlib.closing(connect(remote_port, USER)))
            sql(low_target, f'set role {ROLE}')
            low_id = sql(low_target, 'select connection_id()')[0][0]
            lookup = f'select count(*) from information_schema.processlist where conn_id={low_id}'
            sql(role_actor, "prepare multi_pl from '" + lookup + "'")
            for statement in (lookup, 'execute multi_pl'):
                with role_actor.cursor() as cursor:
                    cursor.execute(f'set role {ROLE}; {statement}; set role moadmin; {statement}; set role {ROLE}; {statement}')
                    results = [tuple(cursor.fetchall())]
                    while cursor.nextset():
                        results.append(tuple(cursor.fetchall()))
                assert results == [(), ((0,),), (), ((1,),), (), ((0,),)], results
            sql(role_actor, 'deallocate prepare multi_pl')
            with request(admin, low_target, 'role') as (cid, future, marker):
                sql(role_actor, "prepare role_pl from 'select conn_id from information_schema.processlist where conn_id=" + str(cid) + "'")
                assert not sql(role_actor, 'execute role_pl')
                rejected(role_actor, f'kill query {cid}')
                assert not future.done() and running(admin, cid, marker)
                sql(role_actor, 'set role moadmin')
                assert sql(role_actor, 'execute role_pl') == ((cid,),)
                sql(role_actor, 'set role ' + ROLE)
                assert not sql(role_actor, 'execute role_pl')
                sql(role_actor, 'deallocate prepare role_pl')
                sql(role_actor, 'set role moadmin')
                sql(role_actor, f'kill query {cid}')
                assert future.result(timeout=10)[0] == 'canceled'
            # A changed user must lose original root authority and retain own-user KILL.
            changed = stack.enter_context(contextlib.closing(connect()))
            change_user(changed, USER, args.password)
            sql(changed, f'set role {ROLE}')
            root_id = sql(target, 'select connection_id()')[0][0]
            visibility(changed, root_id)
            rejected(changed, f'kill query {root_id}')
            failed = stack.enter_context(contextlib.closing(connect()))
            change_user(failed, USER, args.password)
            try:
                change_user(failed, 'root', 'incorrect_session_auth_password')
                raise AssertionError('invalid COM_CHANGE_USER authentication succeeded')
            except pymysql.MySQLError:
                pass
            # The existing frontend contract closes after failed COM_CHANGE_USER.
            try:
                sql(failed, 'select 1')
                raise AssertionError('failed change-user left connection usable')
            except pymysql.MySQLError as error:
                assert error.args[0] in (2006, 2013), str(error)
            try:
                sql(changed, 'set role nonexistent_session_auth_role')
                raise AssertionError('invalid role switch succeeded')
            except pymysql.MySQLError:
                pass
            rejected(changed, f'kill query {root_id}')
            with request(admin, low_target, 'changed_user') as (cid, future, _):
                sql(changed, f'kill query {cid}')
                assert future.result(timeout=10)[0] == 'canceled'
            # Own QUERY, default/CONNECTION, and self semantics remain native SQL.
            own_id = sql(changed, 'select connection_id()')[0][0]
            sql(changed, f'kill query {own_id}')
            assert sql(changed, 'select 1') == ((1,),)
            sql(changed, f'kill {own_id}')
            try:
                sql(changed, 'select 1')
                raise AssertionError('self KILL CONNECTION left connection usable')
            except pymysql.MySQLError:
                pass
            # Disconnect cleanup must cancel a changed ordinary user's running request.
            disconnected = connect()
            change_user(disconnected, USER, args.password)
            sql(disconnected, f'set role {ROLE}')
            cid = sql(disconnected, 'select connection_id()')[0][0]
            disconnected._execute_command(3, 'select sleep(60) /* session_auth_disconnect */')
            deadline = time.monotonic() + 10
            while not running(admin, cid, 'session_auth_disconnect'):
                assert time.monotonic() < deadline
                time.sleep(.02)
            disconnected._force_close()  # No COM_QUIT: exercise in-flight client EOF.
            while sql(admin, 'select conn_id from information_schema.processlist where conn_id=%s', (cid,)):
                assert time.monotonic() < deadline, 'disconnected backend remained alive'
                time.sleep(.02)
            print(json.dumps({'status': 'PASS', 'remote_cn': remote_port,
                              'actor_node': actor_node, 'checks': ['visibility/show/function/view/prepared',
                              'remote query+connection denial without effect', 'role downgrade+upgrade (single and multi-statement, prepared)',
                              'COM_CHANGE_USER denial+own kill', 'failed user+role switches', 'self kill',
                              'changed-user disconnect cancellation']}))
        finally:
            # Close all test sessions before dropping their catalog objects.
            stack.close()
            with contextlib.closing(connect()) as cleanup:
                sql(cleanup, f'drop user if exists {USER}')
                sql(cleanup, f'drop role if exists {ROLE}')
                sql(cleanup, f'drop database if exists {DB}')


if __name__ == '__main__':
    main()
