package idxcron

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

const (
	Type_I64    = "I"
	Type_F64    = "F"
	Type_String = "S"
)

type Metadata struct {
	Data []byte
}

func (m *Metadata) ResolveVariableFunc(varName string, isSystemVar, isGlobalVar bool) (any, error) {

	if m.Data == nil {
		return nil, nil
	}

	var bj bytejson.ByteJson
	if err := bj.Unmarshal(m.Data); err != nil {
		return nil, err
	}

	path, err := bytejson.ParseJsonPath("$.cfg." + varName)
	if err != nil {
		return nil, err
	}

	out := bj.QuerySimple([]*bytejson.Path{&path})
	if out.IsNull() {
		return nil, moerr.NewInternalErrorNoCtx("value is null")
	}

	typepath, err := bytejson.ParseJsonPath("$.t")
	if err != nil {
		return nil, err
	}

	typebj := out.QuerySimple([]*bytejson.Path{&typepath})
	if typebj.IsNull() {
		return nil, moerr.NewInternalErrorNoCtx("type is null")
	}

	valpath, err := bytejson.ParseJsonPath("$.v")
	if err != nil {
		return nil, err
	}

	valbj := out.QuerySimple([]*bytejson.Path{&valpath})
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

type MetadataWriter struct {
	data map[string]any
}

func NewMetadataWriter() *MetadataWriter {
	return &MetadataWriter{data: make(map[string]any)}
}

func (w *MetadataWriter) AddInt(key string, value int64) {
	w.data[key] = value
}

func (w *MetadataWriter) AddString(key string, value string) {
	w.data[key] = value
}

func (w *MetadataWriter) AddFloat(key string, value float64) {
	w.data[key] = value
}

func (w *MetadataWriter) Marshal() string {
	values := make([]string, 0, len(w.data))
	for key, value := range w.data {
		var s string
		switch value.(type) {
		case int64:
			s = fmt.Sprintf(`"%s":{"t":"%s","v":%d}`, key, Type_I64, value)
		case string:
			s = fmt.Sprintf(`"%s":{"t":"%s","v":"%s"}`, key, Type_String, value)
		case float64:
			s = fmt.Sprintf(`"%s":{"t":"%s","v":%f}`, key, Type_F64, value)
		}
		values = append(values, s)
	}

	cfg := strings.Join(values, ",")
	js := fmt.Sprintf(`{"cfg":{%s}}`, cfg)
	return js
}
