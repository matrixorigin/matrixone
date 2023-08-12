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
from concurrent import futures
from typing import Any, Callable

import grpc

import python_udf_pb2_grpc as pb2_grpc
import python_udf_pb2 as pb2


class Server(pb2_grpc.PythonUdfService):

    @staticmethod
    def run(request: pb2.PythonUdfRequest,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None) -> pb2.PythonUdfResponse:
        # 加载函数
        func = loadFunction(request.udf)
        # 初始化返回值
        result = pb2.DataVector(
            const=False,
            data=[],
            length=request.length,
            type=request.udf.retType
        )
        # 计算
        for i in range(request.length):
            params = [None] * len(request.vectors)
            for j in range(len(request.vectors)):
                params[j] = getValueFromDataVector(request.vectors[j], i)
            result.data.append(value2Data(func(*params), result.type))
        return pb2.PythonUdfResponse(vector=result)


def loadFunction(udf: pb2.PythonUdf) -> Callable:
    # 加载函数
    exec(udf.asFun, locals())
    # 获取函数对象
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
    if v.type in [pb2.CHAR, pb2.VARCHAR, pb2.TEXT]:
        return data.stringVal
    if v.type in [pb2.BINARY, pb2.VARBINARY, pb2.BLOB]:
        return data.bytesVal
    else:
        raise Exception("vector type error")


def value2Data(value: Any, typ: pb2.DataType):
    if value is None:
        return pb2.Data()

    if typ == pb2.BOOL:
        return pb2.Data(boolVal=value)
    if typ in [pb2.INT8, pb2.INT16, pb2.INT32]:
        return pb2.Data(intVal=value)
    if typ == pb2.INT64:
        return pb2.Data(int64Val=value)
    if typ in [pb2.UINT8, pb2.INT16, pb2.INT32]:
        return pb2.Data(uintVal=value)
    if typ == pb2.UINT64:
        return pb2.Data(uint64Val=value)
    if typ == pb2.FLOAT32:
        return pb2.Data(floatVal=value)
    if typ == pb2.FLOAT64:
        return pb2.Data(doubleVal=value)
    if typ in [pb2.CHAR, pb2.VARCHAR, pb2.TEXT]:
        return pb2.Data(stringVal=value)
    if typ in [pb2.BINARY, pb2.VARBINARY, pb2.BLOB]:
        return pb2.Data(bytesVal=value)
    else:
        raise Exception("data type error")


def run():
    server = grpc.server(futures.ThreadPoolExecutor())
    pb2_grpc.add_PythonUdfServiceServicer_to_server(Server(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    run()
