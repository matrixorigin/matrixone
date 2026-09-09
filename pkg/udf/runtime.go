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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
)

const (
	LanguagePython       = "python"
	PythonABIContract    = "PYTHON_ARROW"
	PythonAdapterVersion = "2026-09"
	PythonSDKVersion     = "1.0"
	NullCallHandler      = "CALLED_ON_NULL_INPUT"
	NullReturnNull       = "RETURNS_NULL_ON_NULL_INPUT"
)

// Invocation is the language-neutral routine call handed to an admitted
// runtime.  The runtime owns transport, worker lifecycle, and Arrow
// conversion; CN owns the SQL vectors, result wrapper, and fencing identity.
type Invocation struct {
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
	Context        map[string]string
	Tuple          protocol.FencingTuple
}

// Runtime executes one invocation.  Implementations must not retain the
// vectors or result after Execute returns.
type Runtime interface {
	Language() string
	Execute(context.Context, *Invocation, vector.FunctionResultWrapper, *mpool.MPool) error
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
