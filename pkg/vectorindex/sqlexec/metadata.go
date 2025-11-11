// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlexec

import (
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

// Metadata is used to replace ResolveVariableFunc for background job.
// 1. In the frontend, copy all the configurations required with MetadataWriter
// 2. Generate the JSON with MetadataWriter.Marshal() and save it into the database with JSON type
// 3. Use []byte from JSON column to construct ByteJson in Metadata
// 4. set ResolveVaribaleFunc() with Metadata.ResolveVariableFunc() when execute SQL

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

func NewMetadataFromJson(js string) (*Metadata, error) {
	if len(js) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("metadata is null")
	}

	bj, err := bytejson.ParseFromString(js)
	if err != nil {
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
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("key %s not found", varName))
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

func (m *Metadata) Modify(varName string, v any) error {

	if m.bj.IsNull() {
		return moerr.NewInternalErrorNoCtx("bytejson is null")
	}

	bj := m.bj

	path, err := bytejson.ParseJsonPath(fmt.Sprintf("$.cfg.%s", varName))
	if err != nil {
		return err
	}

	var cfgvalue string
	switch v.(type) {
	case float32, float64:
		cfgvalue = fmt.Sprintf(`{"t":"%s", "v":%f}`, Type_F64, v)
	case int, int32, int64:
		cfgvalue = fmt.Sprintf(`{"t":"%s", "v":%d}`, Type_I64, v)
	case string:
		cfgvalue = fmt.Sprintf(`{"t":"%s", "v":"%s"}`, Type_String, v)
	default:
		return moerr.NewInternalErrorNoCtx("invalid value type")
	}

	val, err := bytejson.ParseFromString(cfgvalue)
	if err != nil {
		return err
	}

	bj, err = bj.Modify([]*bytejson.Path{&path}, []bytejson.ByteJson{val}, bytejson.JsonModifySet)
	if err != nil {
		return err
	}

	//fmt.Println(cfgvalue)
	//fmt.Println(bj.String())

	m.bj = bj

	return nil
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
