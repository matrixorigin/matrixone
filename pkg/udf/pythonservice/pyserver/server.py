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
import json
import argparse
import datetime
from concurrent import futures
from typing import Any, Callable, Optional
import decimal

import grpc

import udf_pb2 as pb2
import udf_pb2_grpc as pb2_grpc

DEFAULT_DECIMAL_SCALE = 16

DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATETIME_FORMAT_WITH_PRECISION = '%Y-%m-%d %H:%M:%S.%f'


class Server(pb2_grpc.ServiceServicer):

    def run(self,
            request: pb2.Request,
            context) -> pb2.Response:

        # check
        assert request.language == "python", "udf language must be python"

        # load function
        func = loadFunction(request.udf)

        # set precision
        decimal.getcontext().prec = DEFAULT_DECIMAL_SCALE

        # init result
        result = pb2.DataVector(
            const=False,
            data=[],
            length=request.length,
            type=request.udf.retType,
            scale=defaultScale(request.udf.retType)
        )

        # calculate
        for i in range(request.length):
            params = [None] * len(request.vectors)
            for j in range(len(request.vectors)):
                params[j] = getValueFromDataVector(request.vectors[j], i)
            value = func(*params)
            data = value2Data(value, result.type)
            result.data.append(data)
        return pb2.Response(vector=result, language="python")


def loadFunction(udf: pb2.Udf) -> Callable:
    # load function
    if not udf.isImport:
        exec(udf.body, locals())
    else:
        raise NotImplementedError()
    # get function object
    return locals()[udf.handler]


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
        assert type(value) is datetime.timedelta, f'return type error, required {datetime.timedelta}, received {type(value)}'
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
        assert type(value) is datetime.datetime, f'return type error, required {datetime.datetime}, received {type(value)}'
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
    server = grpc.server(futures.ThreadPoolExecutor())
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
