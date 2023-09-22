# coding = utf-8
# -*- coding:utf-8 -*-
# Copyright 2023 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import datetime
import decimal
import enum
import importlib
import json
import logging
import os
import shutil
import subprocess
import threading
from concurrent import futures
from typing import Any, Callable, Optional, Iterator, Dict

import grpc

import udf_pb2 as pb2
import udf_pb2_grpc as pb2_grpc

DEFAULT_DECIMAL_SCALE = 16

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATETIME_FORMAT_WITH_PRECISION = '%Y-%m-%d %H:%M:%S.%f'

ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
INSTALLED_LABEL = 'installed'

OPTION_VECTOR = 'vector'
OPTION_DECIMAL_PRECISION = 'decimal_precision'

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - [%(name)s] - [%(levelname)s] - [%(threadName)s] : %(message)s'
)
log = logging.getLogger('Server')


class Server(pb2_grpc.ServiceServicer):

    def run(self, requestIterator: Iterator[pb2.Request], context) -> pb2.Response:
        firstRequest: Optional[pb2.Request] = None
        path: Optional[str] = None
        filename: Optional[str] = None
        item: Optional[InstallingItem] = None

        firstBlock = True
        lastBlock = False

        try:
            for request in requestIterator:
                # check
                checkUdf(request.udf)

                # the first request
                if request.type == pb2.DataRequest:
                    log.info('receive data')

                    if request.udf.isImport:
                        path, filename, item, status = functionStatus(request.udf)

                        if status == FunctionStatus.NotExist:
                            firstRequest = request
                            yield pb2.Response(type=pb2.PkgRequest)

                        elif status == FunctionStatus.Installing:
                            with item.condition:
                                # block and waiting
                                if not item.installed:
                                    log.info('waiting')
                                    item.condition.wait()
                                    log.info('finish waiting')
                            yield self.calculate(request, path, filename)

                        else:
                            yield self.calculate(request, path, filename)

                    else:
                        yield self.calculate(request, "", "")

                # the second request (optional)
                # var firstRequest, path, filename and item are not null
                elif request.type == pb2.PkgResponse:
                    log.info('receive pkg')

                    # install pkg, do not need write lock
                    absPath = os.path.join(ROOT_PATH, path)
                    lastBlock = request.udf.importPkg.last
                    try:
                        if firstBlock:
                            if os.path.exists(absPath):
                                shutil.rmtree(absPath)
                            os.makedirs(absPath, exist_ok=True)
                            firstBlock = False

                        file = os.path.join(absPath, filename)

                        with open(file, 'ab+') as f:
                            f.write(request.udf.importPkg.data)

                        if lastBlock:
                            if request.udf.body.endswith('.whl'):
                                subprocess.check_call(['pip', 'install', '--no-index', file, '-t', absPath])
                                os.remove(file)

                            # mark the pkg is installed without error
                            open(os.path.join(absPath, INSTALLED_LABEL), 'w').close()

                    except Exception as e:
                        shutil.rmtree(absPath, ignore_errors=True)
                        raise e

                    finally:
                        if lastBlock:
                            with item.condition:
                                item.installed = True
                                item.condition.notifyAll()

                            with INSTALLING_MAP_LOCK:
                                INSTALLING_MAP[path] = None

                    if not lastBlock:
                        yield pb2.Response(type=pb2.PkgRequest)
                    else:
                        yield self.calculate(firstRequest, path, filename)

                else:
                    raise Exception('error udf request type')
        # notify all
        finally:
            if item is None:
                with INSTALLING_MAP_LOCK:
                    item = INSTALLING_MAP.get(path)

            if item is not None:
                with item.condition:
                    item.installed = True
                    item.condition.notifyAll()

                with INSTALLING_MAP_LOCK:
                    INSTALLING_MAP[path] = None

    def calculate(self, request: pb2.Request, filepath: str, filename: str) -> pb2.Response:
        log.info('calculating')

        # load function
        func = loadFunction(request.udf, filepath, filename)

        # set precision
        if hasattr(func, OPTION_DECIMAL_PRECISION):
            prec = getattr(func, OPTION_DECIMAL_PRECISION)
            if type(prec) is int and prec >= 0:
                decimal.getcontext().prec = prec
            else:
                decimal.getcontext().prec = DEFAULT_DECIMAL_SCALE
        else:
            decimal.getcontext().prec = DEFAULT_DECIMAL_SCALE

        # scalar or vector
        vector = False
        if hasattr(func, OPTION_VECTOR):
            vector = getattr(func, OPTION_VECTOR) is True

        # init result
        result = pb2.DataVector(
            const=False,
            data=[],
            length=request.length,
            type=request.udf.retType,
            scale=defaultScale(request.udf.retType)
        )

        # calculate
        if vector:
            params = []
            for i in range(len(request.vectors)):
                params.append([getValueFromDataVector(request.vectors[i], j) for j in range(request.length)])
            values = func(*params)
            assert len(values) == request.length, f'request length {request.length} is not same with result length {len(values)}'
            for value in values:
                data = value2Data(value, result.type)
                result.data.append(data)
        else:
            for i in range(request.length):
                params = [getValueFromDataVector(request.vectors[j], i) for j in range(len(request.vectors))]
                value = func(*params)
                data = value2Data(value, result.type)
                result.data.append(data)
        log.info('finish calculating')
        return pb2.Response(vector=result, type=pb2.DataResponse)


def checkUdf(udf: pb2.Udf):
    assert udf.handler != "", "udf handler should not be null"
    assert udf.body != "", "udf body should not be null"
    assert udf.language == "python", "udf language should be python"
    assert udf.modifiedTime != "", "udf modifiedTime should not be null"
    assert udf.db != "", "udf db should not be null"


class FunctionStatus(enum.Enum):
    NotExist = 0
    Installing = 1
    Installed = 2


class InstallingItem:
    condition = threading.Condition()
    installed = False


# key: db/func/modified_time, value: InstallingItem
INSTALLING_MAP: Dict[str, Optional[InstallingItem]] = {}
INSTALLING_MAP_LOCK = threading.RLock()


def functionStatus(udf: pb2.Udf) -> (str, str, Optional[InstallingItem], FunctionStatus):
    with INSTALLING_MAP_LOCK:
        filepath, filename = os.path.split(udf.body)
        path = os.path.join('udf', udf.db, filepath[filepath.rfind('/') + 1:], udf.modifiedTime)
        item = INSTALLING_MAP.get(path)
        if item is None:
            if os.path.isfile(os.path.join(ROOT_PATH, path, INSTALLED_LABEL)):
                return path, filename, item, FunctionStatus.Installed
            else:
                item = InstallingItem()
                item.installed = False
                INSTALLING_MAP[path] = item
                return path, filename, item, FunctionStatus.NotExist
        else:
            if item.installed:
                return path, filename, item, FunctionStatus.Installed
            else:
                return path, filename, item, FunctionStatus.Installing


def loadFunction(udf: pb2.Udf, filepath: str, filename: str) -> Callable:
    # load function
    if not udf.isImport:
        exec(udf.body, locals())
        # get function object
        return locals()[udf.handler]
    else:
        if udf.body.endswith('.py'):
            file = importlib.import_module(f'.{filename[:-3]}', package=filepath.replace("/", "."))
            return getattr(file, udf.handler)
        elif udf.body.endswith('.whl'):
            i = udf.handler.rfind('.')
            if i < 1:
                raise Exception(
                    "when you import a *.whl, the handler should be in the format of '<file or module name>.<function name>'")
            file = importlib.import_module(f'.{udf.handler[:i]}', package=filepath.replace("/", "."))
            return getattr(file, udf.handler[i + 1:])


def getDataFromDataVector(v: pb2.DataVector, i: int) -> pb2.Data:
    if v is None:
        return pb2.Data()
    if v.const:
        return v.data[0]
    return v.data[i]


def getValueFromDataVector(v: pb2.DataVector, i: int) -> Any:
    data = getDataFromDataVector(v, i)

    if data.WhichOneof("val") is None:
        return None

    if v.type == pb2.BOOL:
        return data.boolVal
    if v.type in [pb2.INT8, pb2.INT16, pb2.INT32]:
        return data.intVal
    if v.type == pb2.INT64:
        return data.int64Val
    if v.type in [pb2.UINT8, pb2.INT16, pb2.INT32]:
        return data.uintVal
    if v.type == pb2.UINT64:
        return data.uint64Val
    if v.type == pb2.FLOAT32:
        return data.floatVal
    if v.type == pb2.FLOAT64:
        return data.doubleVal
    if v.type in [pb2.CHAR, pb2.VARCHAR, pb2.TEXT, pb2.UUID]:
        return data.stringVal
    if v.type == pb2.JSON:
        return json.loads(data.stringVal)
    if v.type == pb2.TIME:
        sign = 1 if data.stringVal[0] == '-' else 0
        h, m, s = data.stringVal[sign:].split(':')
        if sign == 0:
            return datetime.timedelta(hours=int(h), minutes=int(m), seconds=float(s))
        return datetime.timedelta(hours=-int(h), minutes=-int(m), seconds=-float(s))
    if v.type == pb2.DATE:
        return datetime.datetime.strptime(data.stringVal, DATE_FORMAT).date()
    if v.type in [pb2.DATETIME, pb2.TIMESTAMP]:
        formatStr = DATETIME_FORMAT if v.scale == 0 else DATETIME_FORMAT_WITH_PRECISION
        return datetime.datetime.strptime(data.stringVal, formatStr)
    if v.type in [pb2.DECIMAL64, pb2.DECIMAL128]:
        return decimal.Decimal(data.stringVal)
    if v.type in [pb2.BINARY, pb2.VARBINARY, pb2.BLOB]:
        return data.bytesVal
    else:
        raise Exception("vector type error")


def defaultScale(typ: pb2.DataType, scale: Optional[int] = None) -> int:
    if scale is not None:
        return scale
    if typ in [pb2.FLOAT32, pb2.FLOAT64]:
        return -1
    if typ in [pb2.DECIMAL64, pb2.DECIMAL128]:
        return decimal.getcontext().prec
    if typ in [pb2.TIME, pb2.DATETIME, pb2.TIMESTAMP]:
        return 6
    return 0


def value2Data(value: Any, typ: pb2.DataType) -> pb2.Data:
    if value is None:
        return pb2.Data()

    if typ == pb2.BOOL:
        assert type(value) is bool, f'return type error, required {bool}, received {type(value)}'
        return pb2.Data(boolVal=value)
    if typ in [pb2.INT8, pb2.INT16, pb2.INT32]:
        assert type(value) is int, f'return type error, required {int}, received {type(value)}'
        return pb2.Data(intVal=value)
    if typ == pb2.INT64:
        assert type(value) is int, f'return type error, required {int}, received {type(value)}'
        return pb2.Data(int64Val=value)
    if typ in [pb2.UINT8, pb2.INT16, pb2.INT32]:
        assert type(value) is int, f'return type error, required {int}, received {type(value)}'
        return pb2.Data(uintVal=value)
    if typ == pb2.UINT64:
        assert type(value) is int, f'return type error, required {int}, received {type(value)}'
        return pb2.Data(uint64Val=value)
    if typ == pb2.FLOAT32:
        assert type(value) is float, f'return type error, required {float}, received {type(value)}'
        return pb2.Data(floatVal=value)
    if typ == pb2.FLOAT64:
        assert type(value) is float, f'return type error, required {float}, received {type(value)}'
        return pb2.Data(doubleVal=value)
    if typ in [pb2.CHAR, pb2.VARCHAR, pb2.TEXT, pb2.UUID]:
        assert type(value) is str, f'return type error, required {str}, received {type(value)}'
        return pb2.Data(stringVal=value)
    if typ == pb2.JSON:
        return pb2.Data(stringVal=json.dumps(value, ensure_ascii=False))
    if typ == pb2.TIME:
        assert type(
            value) is datetime.timedelta, f'return type error, required {datetime.timedelta}, received {type(value)}'
        r = ''
        t: datetime.timedelta = value
        if t.days < 0:
            t = t * -1
            r += '-'
        h = t.days * 24 + t.seconds // 3600
        m = t.seconds % 3600 // 60
        s = t.seconds % 3600 % 60
        r += f'{h:02d}:{m:02d}:{s:02d}.{t.microseconds:06d}'
        return pb2.Data(stringVal=str(r))
    if typ == pb2.DATE:
        assert type(value) is datetime.date, f'return type error, required {datetime.date}, received {type(value)}'
        return pb2.Data(stringVal=str(value))
    if typ in [pb2.DATETIME, pb2.TIMESTAMP]:
        assert type(
            value) is datetime.datetime, f'return type error, required {datetime.datetime}, received {type(value)}'
        return pb2.Data(stringVal=str(value))
    if typ in [pb2.DECIMAL64, pb2.DECIMAL128]:
        assert type(value) is decimal.Decimal, f'return type error, required {decimal.Decimal}, received {type(value)}'
        return pb2.Data(stringVal=str(value))
    if typ in [pb2.BINARY, pb2.VARBINARY, pb2.BLOB]:
        assert type(value) is bytes, f'return type error, required {bytes}, received {type(value)}'
        return pb2.Data(bytesVal=value)
    else:
        raise Exception(f'unsupported return type: {type(value)}')


def run():
    server = grpc.server(futures.ThreadPoolExecutor(), options=[
        ('grpc.max_send_message_length', 0x7fffffff),
        ('grpc.max_receive_message_length', 0x7fffffff)
    ])
    pb2_grpc.add_ServiceServicer_to_server(Server(), server)
    server.add_insecure_port(ARGS.address)
    server.start()
    server.wait_for_termination()


ARGS = None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='python udf server',
        epilog='Copyright(r), 2023'
    )
    parser.add_argument('--address', default='[::]:50051', help='address')
    ARGS = parser.parse_args()
    run()
