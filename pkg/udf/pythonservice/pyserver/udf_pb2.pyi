from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RequestType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    UnknownRequest: _ClassVar[RequestType]
    DataRequest: _ClassVar[RequestType]
    PkgResponse: _ClassVar[RequestType]

class ResponseType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    UnknownResponse: _ClassVar[ResponseType]
    DataResponse: _ClassVar[ResponseType]
    PkgRequest: _ClassVar[ResponseType]

class DataType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    UNKNOWN: _ClassVar[DataType]
    BOOL: _ClassVar[DataType]
    INT8: _ClassVar[DataType]
    INT16: _ClassVar[DataType]
    INT32: _ClassVar[DataType]
    INT64: _ClassVar[DataType]
    UINT8: _ClassVar[DataType]
    UINT16: _ClassVar[DataType]
    UINT32: _ClassVar[DataType]
    UINT64: _ClassVar[DataType]
    FLOAT32: _ClassVar[DataType]
    FLOAT64: _ClassVar[DataType]
    CHAR: _ClassVar[DataType]
    VARCHAR: _ClassVar[DataType]
    TEXT: _ClassVar[DataType]
    JSON: _ClassVar[DataType]
    UUID: _ClassVar[DataType]
    TIME: _ClassVar[DataType]
    DATE: _ClassVar[DataType]
    DATETIME: _ClassVar[DataType]
    TIMESTAMP: _ClassVar[DataType]
    DECIMAL64: _ClassVar[DataType]
    DECIMAL128: _ClassVar[DataType]
    BINARY: _ClassVar[DataType]
    VARBINARY: _ClassVar[DataType]
    BLOB: _ClassVar[DataType]
UnknownRequest: RequestType
DataRequest: RequestType
PkgResponse: RequestType
UnknownResponse: ResponseType
DataResponse: ResponseType
PkgRequest: ResponseType
UNKNOWN: DataType
BOOL: DataType
INT8: DataType
INT16: DataType
INT32: DataType
INT64: DataType
UINT8: DataType
UINT16: DataType
UINT32: DataType
UINT64: DataType
FLOAT32: DataType
FLOAT64: DataType
CHAR: DataType
VARCHAR: DataType
TEXT: DataType
JSON: DataType
UUID: DataType
TIME: DataType
DATE: DataType
DATETIME: DataType
TIMESTAMP: DataType
DECIMAL64: DataType
DECIMAL128: DataType
BINARY: DataType
VARBINARY: DataType
BLOB: DataType

class Request(_message.Message):
    __slots__ = ["udf", "vectors", "length", "type"]
    UDF_FIELD_NUMBER: _ClassVar[int]
    VECTORS_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    udf: Udf
    vectors: _containers.RepeatedCompositeFieldContainer[DataVector]
    length: int
    type: RequestType
    def __init__(self, udf: _Optional[_Union[Udf, _Mapping]] = ..., vectors: _Optional[_Iterable[_Union[DataVector, _Mapping]]] = ..., length: _Optional[int] = ..., type: _Optional[_Union[RequestType, str]] = ...) -> None: ...

class Response(_message.Message):
    __slots__ = ["vector", "type"]
    VECTOR_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    vector: DataVector
    type: ResponseType
    def __init__(self, vector: _Optional[_Union[DataVector, _Mapping]] = ..., type: _Optional[_Union[ResponseType, str]] = ...) -> None: ...

class Udf(_message.Message):
    __slots__ = ["handler", "isImport", "body", "importPkg", "retType", "language", "modifiedTime", "db"]
    HANDLER_FIELD_NUMBER: _ClassVar[int]
    ISIMPORT_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    IMPORTPKG_FIELD_NUMBER: _ClassVar[int]
    RETTYPE_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    MODIFIEDTIME_FIELD_NUMBER: _ClassVar[int]
    DB_FIELD_NUMBER: _ClassVar[int]
    handler: str
    isImport: bool
    body: str
    importPkg: bytes
    retType: DataType
    language: str
    modifiedTime: str
    db: str
    def __init__(self, handler: _Optional[str] = ..., isImport: bool = ..., body: _Optional[str] = ..., importPkg: _Optional[bytes] = ..., retType: _Optional[_Union[DataType, str]] = ..., language: _Optional[str] = ..., modifiedTime: _Optional[str] = ..., db: _Optional[str] = ...) -> None: ...

class DataVector(_message.Message):
    __slots__ = ["data", "const", "length", "type", "scale"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    CONST_FIELD_NUMBER: _ClassVar[int]
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedCompositeFieldContainer[Data]
    const: bool
    length: int
    type: DataType
    scale: int
    def __init__(self, data: _Optional[_Iterable[_Union[Data, _Mapping]]] = ..., const: bool = ..., length: _Optional[int] = ..., type: _Optional[_Union[DataType, str]] = ..., scale: _Optional[int] = ...) -> None: ...

class Data(_message.Message):
    __slots__ = ["boolVal", "intVal", "int64Val", "uintVal", "uint64Val", "floatVal", "doubleVal", "stringVal", "bytesVal"]
    BOOLVAL_FIELD_NUMBER: _ClassVar[int]
    INTVAL_FIELD_NUMBER: _ClassVar[int]
    INT64VAL_FIELD_NUMBER: _ClassVar[int]
    UINTVAL_FIELD_NUMBER: _ClassVar[int]
    UINT64VAL_FIELD_NUMBER: _ClassVar[int]
    FLOATVAL_FIELD_NUMBER: _ClassVar[int]
    DOUBLEVAL_FIELD_NUMBER: _ClassVar[int]
    STRINGVAL_FIELD_NUMBER: _ClassVar[int]
    BYTESVAL_FIELD_NUMBER: _ClassVar[int]
    boolVal: bool
    intVal: int
    int64Val: int
    uintVal: int
    uint64Val: int
    floatVal: float
    doubleVal: float
    stringVal: str
    bytesVal: bytes
    def __init__(self, boolVal: bool = ..., intVal: _Optional[int] = ..., int64Val: _Optional[int] = ..., uintVal: _Optional[int] = ..., uint64Val: _Optional[int] = ..., floatVal: _Optional[float] = ..., doubleVal: _Optional[float] = ..., stringVal: _Optional[str] = ..., bytesVal: _Optional[bytes] = ...) -> None: ...
