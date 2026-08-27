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
	"strings"

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
	Type_I8     = "I8"
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
	bj.Unmarshal(data)

	typepath, _ := bytejson.ParseJsonPath("$.t")
	valpath, _ := bytejson.ParseJsonPath("$.v")

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

	typepath, _ := bytejson.ParseJsonPath("$.t")
	valpath, _ := bytejson.ParseJsonPath("$.v")

	return &Metadata{bj: bj, typepath: typepath, valpath: valpath}, nil
}

func (m *Metadata) GetString() string {
	return m.bj.String()
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
	case Type_I8:
		return int8(valbj.GetInt64()), nil
	case Type_I64:
		return valbj.GetInt64(), nil
	case Type_F64:
		return valbj.GetFloat64(), nil
	case Type_String:
		return string(valbj.GetString()), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("invalid configuration type")
}

// sessionSystemVarDefaults is the authoritative enumeration of the session/system
// variables a background reindex resolves that its captured Metadata does NOT
// hold, each mapped to the correct background default. It is deliberately an
// explicit whitelist, not a catch-all: a var that is neither captured nor listed
// here fails fast (see ResolveVariableWithSessionDefaults) so a newly-plumbed
// dependency surfaces loudly, by name, and gets a deliberate default rather than
// silently resolving to nil.
//
//   - sql_mode: "" — #25438 wired it into the zero-temporal-date write-policy
//     check (process/eval_expr_util.RejectZeroTemporalWritePolicy). "" (no strict /
//     no zero-date modes) is the correct permissive value for a background reindex,
//     matching backSession.GetSessionSysVar, and safer than capturing the user's
//     real (possibly strict) sql_mode, which could make the rebuild reject rows
//     already living in the base table.
//   - lock_wait_timeout: nil — its callers (lockop, process_codec) fall back to
//     their own context-aware default (txnTimeout / procSessionLockWaitTimeout) on
//     nil, which a fixed value would wrongly override; this matches backSession,
//     which returns nil for it too.
var sessionSystemVarDefaults = map[string]any{
	"sql_mode":          "",
	"lock_wait_timeout": nil,
}

// captured reports whether varName was written into this blob's cfg — i.e. it is
// one of the algo build knobs snapshotted at CREATE. An uncaptured name is a
// session/system var the Metadata was never meant to hold.
func (m *Metadata) captured(varName string) bool {
	if m.bj.IsNull() {
		return false
	}
	path, err := bytejson.ParseJsonPath("$.cfg." + varName)
	if err != nil {
		return false
	}
	out := m.bj.QuerySimple([]*bytejson.Path{&path})
	return !out.IsNull()
}

// ResolveVariableWithSessionDefaults is the ResolveVariableFunc to install for a
// background job that runs through a captured Metadata (the idxcron reindex
// hook). A background reindex resolves session/system vars its capture blob never
// held — sql_mode, lock_wait_timeout, and whatever the next release plumbs in —
// and the strict ResolveVariableFunc would error "key X not found" on every one,
// aborting the reindex the moment a non-graceful caller (like the sql_mode
// write-policy check) hits it.
//
// Resolution order:
//  1. CAPTURED var → resolve strictly; a malformed algo knob still errors, so a
//     genuine build-config bug surfaces loudly.
//  2. Known session var (sessionSystemVarDefaults) → its background default.
//  3. Otherwise FAIL FAST, naming the var. This is deliberate: rather than
//     silently defaulting an un-enumerated var to nil, we force a newly-plumbed
//     session-var dependency to surface with a clear reason so it gets added to
//     sessionSystemVarDefaults with a correct, deliberate default.
func (m *Metadata) ResolveVariableWithSessionDefaults(varName string, isSystemVar, isGlobalVar bool) (any, error) {
	if m.captured(varName) {
		return m.ResolveVariableFunc(varName, isSystemVar, isGlobalVar)
	}
	if def, ok := sessionSystemVarDefaults[strings.ToLower(varName)]; ok {
		return def, nil
	}
	return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
		"idxcron reindex resolved un-enumerated session variable %q: add it to "+
			"sqlexec.sessionSystemVarDefaults with the correct background default",
		varName))
}

// ResolveVariableSoft resolves a captured var and returns (nil, nil) for any var
// the blob does not hold — no error, no log. It is for consumers that treat
// "absent" as "use the caller's own default" and must NOT emit spurious error
// logs, e.g. indexplugin.AlgoParamInt (flat key → session var → default), which
// already handles nil gracefully. This is deliberately distinct from
// ResolveVariableWithSessionDefaults (the idxcron reindex resolver), which
// fail-fasts on an un-enumerated var to surface new session-var dependencies.
func (m *Metadata) ResolveVariableSoft(varName string, isSystemVar, isGlobalVar bool) (any, error) {
	if m.captured(varName) {
		return m.ResolveVariableFunc(varName, isSystemVar, isGlobalVar)
	}
	return nil, nil
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
	case int8:
		cfgvalue = fmt.Sprintf(`{"t":"%s", "v":%d}`, Type_I8, v)
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

func (w *MetadataWriter) AddInt8(key string, value int8) {
	w.Cfg[key] = ConfigValue{T: Type_I8, V: value}
}

func (w *MetadataWriter) AddString(key string, value string) {
	w.Cfg[key] = ConfigValue{T: Type_String, V: value}
}

func (w *MetadataWriter) AddFloat(key string, value float64) {
	w.Cfg[key] = ConfigValue{T: Type_F64, V: value}
}

// metadataMarshaler emits sorted map keys so the serialized blob is
// deterministic. The Cfg map would otherwise marshal in random Go iteration
// order, making algo_params.session_vars (and the idxcron metadata) differ
// build-to-build — and BVT .results comparing algo_params flaky. Values are
// read back by key, so the ordering is purely cosmetic.
var metadataMarshaler = sonic.Config{SortMapKeys: true}.Froze()

func (w *MetadataWriter) Marshal() ([]byte, error) {
	return metadataMarshaler.Marshal(w)
}
