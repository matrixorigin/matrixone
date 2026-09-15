// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package udf contains the execution contract shared by SQL routine binders,
// CN operators, and language runtimes.  Wire formats and language-specific
// adapters live below this package; the SQL engine depends only on this
// contract.
package udf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
)

const (
	LanguageSQL          = "sql"
	LanguagePython       = "python"
	PythonABIContract    = "PYTHON_ARROW"
	PythonAdapterVersion = "2026-09"
	PythonSDKVersion     = "1.0"
	// Python materializes statement timestamps as datetime.datetime. Keep the
	// Go boundary identical to Python's year-1 through year-9999 UTC range;
	// accepting the full int64 domain here would let a plan pass CN validation
	// and fail only after the worker starts decoding its context.
	minStatementTimestampUTC int64 = -62135596800000000
	maxStatementTimestampUTC int64 = 253402300799999999
	// These numeric contracts are persisted in catalog and plan records. They
	// are intentionally separate from the human-facing adapter/SDK names:
	// changing a wire or persistent shape must fail closed before user code is
	// entered, even when the runtime happens to use the same language ABI name.
	SQLDefinitionSchemaVersion    = 1
	RoutinePlanContractVersion    = 1
	PythonDefinitionSchemaVersion = 1
	// Kept as a source-compatible alias for the Python adapter while the
	// plan field is shared by SQL and Python routine calls.
	PythonPlanContractVersion = RoutinePlanContractVersion
	// PythonSignatureKeySchemaVersion versions the canonical descriptor
	// encoding stored on the shared Function identity. It is independent of
	// the implementation body schema: changing the overload key is an
	// identity/catalog migration, while changing the body is a revision.
	PythonSignatureKeySchemaVersion = 1
	PythonTypeDescriptorContract    = "ARROW_DESCRIPTOR"
	NullCallHandler                 = "CALLED_ON_NULL_INPUT"
	NullReturnNull                  = "RETURNS_NULL_ON_NULL_INPUT"
	StatementContextContractVersion = 1
	SecurityFrameContractVersion    = 1
)

// StatementContext is the typed execution-time snapshot visible to a Python
// handler. It is intentionally separate from plan metadata: a prepared plan
// may be reused by many statements, while these values are frozen once per
// execution. The JSON form is also the cross-process contract used by the
// Gateway and worker.
type StatementContext struct {
	ContractVersion         int      `json:"contract_version"`
	StatementTimestampUTC   int64    `json:"statement_timestamp_utc"`
	TimezoneKind            string   `json:"timezone_kind"`
	TimezoneName            string   `json:"timezone_name,omitempty"`
	TimezoneOffsetMinutes   int32    `json:"timezone_offset_minutes"`
	TimezoneDatabaseVersion string   `json:"timezone_database_version,omitempty"`
	SQLMode                 []string `json:"sql_mode"`
	CurrentDatabase         string   `json:"current_database,omitempty"`
	CurrentUser             string   `json:"current_user"`
	CurrentRole             string   `json:"current_role,omitempty"`
	ConnectionCollation     string   `json:"connection_collation"`
}

func (c StatementContext) Validate() error {
	if c.ContractVersion != StatementContextContractVersion {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported statement context contract %d", c.ContractVersion)
	}
	if c.StatementTimestampUTC < minStatementTimestampUTC || c.StatementTimestampUTC > maxStatementTimestampUTC {
		return fmt.Errorf("python udf: statement timestamp is outside the Python datetime range")
	}
	if c.CurrentUser == "" || c.ConnectionCollation == "" {
		return fmt.Errorf("python udf: statement context has no authenticated user or collation")
	}
	if c.TimezoneKind == "IANA" {
		if c.TimezoneName == "" || c.TimezoneDatabaseVersion == "" || c.TimezoneOffsetMinutes != 0 {
			return fmt.Errorf("python udf: IANA statement timezone is incomplete")
		}
	} else if c.TimezoneKind == "FIXED_OFFSET" {
		if c.TimezoneName != "" || c.TimezoneDatabaseVersion != "" || c.TimezoneOffsetMinutes < -839 || c.TimezoneOffsetMinutes > 840 {
			return fmt.Errorf("python udf: fixed statement timezone is invalid")
		}
	} else {
		return fmt.Errorf("python udf: unsupported statement timezone kind %q", c.TimezoneKind)
	}
	mode := append([]string(nil), c.SQLMode...)
	sort.Strings(mode)
	for i := range mode {
		if mode[i] == "" || (i > 0 && mode[i] == mode[i-1]) {
			return fmt.Errorf("python udf: statement sql_mode is not canonical")
		}
	}
	if len(mode) != len(c.SQLMode) {
		return fmt.Errorf("python udf: statement sql_mode is not canonical")
	}
	for i := range mode {
		if mode[i] != c.SQLMode[i] {
			return fmt.Errorf("python udf: statement sql_mode is not sorted")
		}
	}
	return nil
}

// StatementContextFromMap is the only adapter from the legacy internal map
// assembled by the SQL session to the typed runtime contract. It accepts the
// map solely at the trusted CN boundary and returns nil when no statement
// fields are present, which keeps transport/protocol unit tests able to test
// zero-row admission without manufacturing session state.
func StatementContextFromMap(values map[string]string) (*StatementContext, error) {
	if len(values) == 0 {
		return nil, nil
	}
	statementKeys := []string{
		"statement_timestamp_utc", "session_timezone_kind", "session_timezone_name",
		"session_timezone_offset_minutes", "session_timezone_tzdb_version", "sql_mode",
		"current_database", "current_user", "current_role", "connection_collation",
	}
	present := false
	for _, key := range statementKeys {
		if _, ok := values[key]; ok {
			present = true
			break
		}
	}
	if !present {
		return nil, nil
	}
	timestamp, err := strconv.ParseInt(values["statement_timestamp_utc"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("python udf: invalid statement timestamp: %w", err)
	}
	mode := make([]string, 0)
	if raw := values["sql_mode"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &mode); err != nil {
			return nil, fmt.Errorf("python udf: invalid statement sql_mode: %w", err)
		}
		// The wire contract is a JSON array, including when it is empty.  A
		// JSON null decodes successfully into a nil Go slice, but would be
		// serialized back as null and rejected by the Python context decoder.
		// Reject it at the trusted CN adapter instead of carrying a malformed
		// snapshot into the Flight request.
		if mode == nil {
			return nil, fmt.Errorf("python udf: invalid statement sql_mode: expected a JSON array")
		}
	}
	context := &StatementContext{
		ContractVersion:         StatementContextContractVersion,
		StatementTimestampUTC:   timestamp,
		TimezoneKind:            strings.ToUpper(values["session_timezone_kind"]),
		TimezoneName:            values["session_timezone_name"],
		SQLMode:                 mode,
		CurrentDatabase:         values["current_database"],
		CurrentUser:             values["current_user"],
		CurrentRole:             values["current_role"],
		ConnectionCollation:     values["connection_collation"],
		TimezoneDatabaseVersion: values["session_timezone_tzdb_version"],
	}
	if raw := values["session_timezone_offset_minutes"]; raw != "" {
		offset, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil {
			return nil, fmt.Errorf("python udf: invalid fixed timezone offset: %w", parseErr)
		}
		context.TimezoneOffsetMinutes = int32(offset)
	}
	if err := context.Validate(); err != nil {
		return nil, err
	}
	return context, nil
}

// SecurityFrame records the principal entering this routine layer. Python
// currently supports INVOKER only, so the effective IDs must equal the
// invoker IDs. The explicit frame prevents a future definer implementation
// from silently reusing the outer session's principal.
type SecurityFrame struct {
	ContractVersion int    `json:"contract_version"`
	Mode            string `json:"mode"`
	InvokerUserID   uint32 `json:"invoker_user_id"`
	InvokerRoleID   uint32 `json:"invoker_role_id"`
	EffectiveUserID uint32 `json:"effective_user_id"`
	EffectiveRoleID uint32 `json:"effective_role_id"`
}

func (f SecurityFrame) Validate() error {
	if f.ContractVersion != SecurityFrameContractVersion || f.Mode != "INVOKER" {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: unsupported Python security frame")
	}
	if f.InvokerUserID != f.EffectiveUserID || f.InvokerRoleID != f.EffectiveRoleID {
		return fmt.Errorf("UNSUPPORTED_ROUTINE_VERSION: Python invoker security frame changed effective principal")
	}
	return nil
}

// Invocation is the language-neutral routine call handed to an admitted
// runtime.  The runtime owns transport, worker lifecycle, and Arrow
// conversion; CN owns the SQL vectors, result wrapper, and fencing identity.
type Invocation struct {
	// Function identity and revision are copied from the typed plan. They are
	// part of the execution grant sent to the worker; the runtime never
	// resolves a function name or latest revision.
	FunctionRef    FunctionRef
	Language       string
	Handler        string
	Source         string
	Args           []types.Type
	ReturnType     types.Type
	Inputs         []*vector.Vector
	Length         int
	Mode           string
	NullPolicy     string
	ABIContract    string
	AdapterVersion string
	SDKVersion     string
	// These values are copied from the immutable typed implementation. The
	// gateway and worker validate them before opening user code; they are not
	// inferred from a mutable source path or a function name.
	DefinitionSchemaVersion int
	ArtifactDigest          string
	EnvironmentDigest       string
	DefinitionFingerprint   string
	Context                 map[string]string
	// Common routine semantics are copied from the typed plan. The Python
	// gateway refuses an invocation with absent or altered values.
	CallsiteID       string
	MayError         bool
	SecurityMode     string
	Leakproof        bool
	StatementContext *StatementContext
	SecurityFrame    *SecurityFrame
	Tuple            protocol.FencingTuple
}

// FunctionRef is the language-neutral form of the shared Catalog identity
// carried through the runtime boundary.
type FunctionRef struct {
	AccountID        uint64 `json:"account_id"`
	DatabaseID       uint64 `json:"database_id"`
	FunctionID       uint64 `json:"function_id"`
	Revision         uint64 `json:"revision"`
	NamespaceVersion uint64 `json:"namespace_version"`
}

func (r FunctionRef) Validate() error {
	if r.DatabaseID == 0 || r.FunctionID == 0 || r.Revision == 0 || r.NamespaceVersion == 0 {
		return moerr.NewInvalidInputNoCtx("incomplete UDF FunctionRef")
	}
	return nil
}

// Runtime executes one invocation.  Implementations must not retain the
// vectors or result after Execute returns.
type Runtime interface {
	Language() string
	Execute(context.Context, *Invocation, vector.FunctionResultWrapper, *mpool.MPool) error
}

// RuntimeCloser is an optional lifecycle contract for runtimes that own a
// transport connection or a worker process.  Execution callers only need
// Runtime; service shutdown uses this interface to close such resources.
type RuntimeCloser interface {
	Runtime
	Close() error
}

// RuntimeReadiness is an optional feature-level gate. A caller uses it before
// publishing a routine so a node never persists a new external definition
// that its current runtime cannot execute. It does not affect ordinary SQL
// routines or ordinary SQL when the Python runtime is disabled.
type RuntimeReadiness interface {
	CheckLanguageReady(context.Context, string) error
}

// RoutineDefinition is the typed, pre-publication contract handed to a
// language runtime for definition validation. It is deliberately separate
// from Invocation: a CREATE or REPLACE has no FunctionRef/revision yet and
// must be validated before the new Catalog revision becomes visible.
//
// Source is transient create-time input. An executable plan never carries
// source; a runtime with an ArtifactResolver must resolve the same immutable
// artifact digest before sending source to the worker.
type RoutineDefinition struct {
	Language                string
	AccountID               uint64
	Handler                 string
	Source                  string
	Args                    []types.Type
	ReturnType              types.Type
	Mode                    string
	NullPolicy              string
	ABIContract             string
	AdapterVersion          string
	SDKVersion              string
	DefinitionSchemaVersion int
	ArtifactDigest          string
	EnvironmentDigest       string
	DefinitionFingerprint   string
}

// RuntimeDefinitionValidator is an optional language runtime capability.
// Python CREATE/REPLACE requires it: the worker must compile the exact
// immutable artifact before Catalog publication. Keeping this as a separate
// capability preserves the language-neutral execution interface and lets
// ordinary SQL UDFs continue to use Runtime implementations that do not own
// a source validation protocol.
type RuntimeDefinitionValidator interface {
	ValidateDefinition(context.Context, *RoutineDefinition) error
}

// Registry dispatches to the one runtime selected by a routine's language.
// A registry is immutable after construction, so plan execution cannot race
// service registration or observe a partially initialized adapter.
func NewRuntime(runtimes ...Runtime) (Runtime, error) {
	registry := &runtimeRegistry{byLanguage: make(map[string]Runtime, len(runtimes))}
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Language() == "" {
			return nil, moerr.NewInternalErrorNoCtx("invalid udf runtime")
		}
		if _, exists := registry.byLanguage[runtime.Language()]; exists {
			return nil, moerr.NewInternalErrorNoCtx("too many " + runtime.Language() + " runtimes")
		}
		registry.byLanguage[runtime.Language()] = runtime
	}
	return registry, nil
}

type runtimeRegistry struct {
	byLanguage map[string]Runtime
	closeOnce  sync.Once
	closeErr   error
}

func (r *runtimeRegistry) Language() string { return "multiple" }

func (r *runtimeRegistry) Execute(
	ctx context.Context,
	invocation *Invocation,
	result vector.FunctionResultWrapper,
	mp *mpool.MPool,
) error {
	if invocation == nil {
		return moerr.NewInternalError(ctx, "nil udf invocation")
	}
	runtime := r.byLanguage[invocation.Language]
	if runtime == nil {
		return moerr.NewInternalError(ctx, "missing "+invocation.Language+" udf runtime")
	}
	return runtime.Execute(ctx, invocation, result, mp)
}

func (r *runtimeRegistry) CheckLanguageReady(ctx context.Context, language string) error {
	runtime := r.byLanguage[language]
	if runtime == nil {
		return moerr.NewNotSupportedf(ctx, "%s UDF runtime is not enabled", language)
	}
	readiness, ok := runtime.(RuntimeReadiness)
	if !ok {
		return moerr.NewNotSupportedf(ctx, "%s UDF runtime does not expose the current contract", language)
	}
	return readiness.CheckLanguageReady(ctx, language)
}

func (r *runtimeRegistry) ValidateDefinition(ctx context.Context, definition *RoutineDefinition) error {
	if definition == nil {
		return moerr.NewInvalidInputNoCtx("nil UDF routine definition")
	}
	runtime := r.byLanguage[definition.Language]
	if runtime == nil {
		return moerr.NewNotSupportedf(ctx, "%s UDF runtime is not enabled", definition.Language)
	}
	validator, ok := runtime.(RuntimeDefinitionValidator)
	if !ok {
		return moerr.NewNotSupportedf(ctx, "%s UDF runtime does not expose definition validation", definition.Language)
	}
	return validator.ValidateDefinition(ctx, definition)
}

func (r *runtimeRegistry) Close() error {
	r.closeOnce.Do(func() {
		languages := make([]string, 0, len(r.byLanguage))
		for language := range r.byLanguage {
			languages = append(languages, language)
		}
		sort.Strings(languages)
		for _, language := range languages {
			if closer, ok := r.byLanguage[language].(RuntimeCloser); ok {
				r.closeErr = errors.Join(r.closeErr, closer.Close())
			}
		}
	})
	return r.closeErr
}
