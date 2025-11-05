package idxcron

import (
	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

const (
	Type_I64    = "I"
	Type_F64    = "F"
	Type_String = "S"
)

type Metadata struct {
	bj       bytejson.ByteJson
	typepath bytejson.Path
	valpath  bytejson.Path
}

func NewMetadata(data []byte) (*Metadata, error) {
	if data == nil {
		return nil, moerr.NewInternalErrorNoCtx("metadata is null")
	}

	var bj bytejson.ByteJson
	if err := bj.Unmarshal(data); err != nil {
		return nil, err
	}

	typepath, err := bytejson.ParseJsonPath("$.t")
	if err != nil {
		return nil, err
	}

	valpath, err := bytejson.ParseJsonPath("$.v")
	if err != nil {
		return nil, err
	}

	return &Metadata{bj: bj, typepath: typepath, valpath: valpath}, nil
}

func (m *Metadata) ResolveVariableFunc(varName string, isSystemVar, isGlobalVar bool) (any, error) {

	if m.bj.IsNull() {
		return nil, nil
	}

	bj := m.bj

	path, err := bytejson.ParseJsonPath("$.cfg." + varName)
	if err != nil {
		return nil, err
	}

	out := bj.QuerySimple([]*bytejson.Path{&path})
	if out.IsNull() {
		return nil, moerr.NewInternalErrorNoCtx("value is null")
	}

	typebj := out.QuerySimple([]*bytejson.Path{&m.typepath})
	if typebj.IsNull() {
		return nil, moerr.NewInternalErrorNoCtx("type is null")
	}

	valbj := out.QuerySimple([]*bytejson.Path{&m.valpath})
	if valbj.IsNull() {
		return nil, moerr.NewInternalErrorNoCtx("value is null")
	}

	switch string(typebj.GetString()) {
	case Type_I64:
		return valbj.GetInt64(), nil
	case Type_F64:
		return valbj.GetFloat64(), nil
	case Type_String:
		return string(valbj.GetString()), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("invalid configuration type")
}

type ConfigValue struct {
	T string `json:"t"`
	V any    `json:"v"`
}

type MetadataWriter struct {
	Cfg map[string]ConfigValue `json:"cfg"`
}

func NewMetadataWriter() *MetadataWriter {
	return &MetadataWriter{Cfg: make(map[string]ConfigValue)}
}

func (w *MetadataWriter) AddInt(key string, value int64) {
	w.Cfg[key] = ConfigValue{T: Type_I64, V: value}
}

func (w *MetadataWriter) AddString(key string, value string) {
	w.Cfg[key] = ConfigValue{T: Type_String, V: value}
}

func (w *MetadataWriter) AddFloat(key string, value float64) {
	w.Cfg[key] = ConfigValue{T: Type_F64, V: value}
}

func (w *MetadataWriter) Marshal() ([]byte, error) {
	return sonic.Marshal(w)
}
