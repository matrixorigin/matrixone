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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// esql_tvf_connect / sql_tvf_connect open (or reuse) a session-cached foreign
// connection from a JSON config and return its handle. A NULL/empty config
// falls back to the @esql_tvf_config / @sql_tvf_config session variable. These
// are marked volatile so they are evaluated for their connect side effect and
// never constant-folded.

func builtInEsqlTvfConnect(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return foreignTvfConnect(parameters, result, proc, length, foreigntvf.KindESQL)
}

func builtInSqlTvfConnect(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return foreignTvfConnect(parameters, result, proc, length, foreigntvf.KindSQL)
}

func foreignTvfConnect(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, kind foreigntvf.Kind) error {
	cache, ok := proc.GetSession().(process.ForeignConnCache)
	if !ok {
		return moerr.NewInvalidInput(proc.Ctx, "esql_tvf_connect/sql_tvf_connect requires an interactive session")
	}
	rs := vector.MustFunctionResult[types.Varlena](result)
	cfgParam := vector.GenerateFunctionStrParameter(parameters[0])
	for i := uint64(0); i < uint64(length); i++ {
		cfg, cfgNull := cfgParam.GetStrValue(i)
		var configStr string
		if cfgNull || len(cfg) == 0 {
			s, err := foreigntvf.ConfigFromSessionVar(proc.Ctx, proc, kind)
			if err != nil {
				return err
			}
			configStr = s
		} else {
			configStr = string(cfg)
		}
		_, handle, err := foreigntvf.ResolveOrConnect(proc.Ctx, cache, kind, configStr)
		if err != nil {
			return err
		}
		if err := rs.AppendBytes([]byte(handle), false); err != nil {
			return err
		}
	}
	return nil
}

// esql_tvf_disconnect / sql_tvf_disconnect close and remove a cached connection
// by handle, returning true if a connection was removed, false if the handle
// was unknown (already disconnected), and NULL for a NULL handle.

func builtInEsqlTvfDisconnect(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return foreignTvfDisconnect(parameters, result, proc, length)
}

func builtInSqlTvfDisconnect(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return foreignTvfDisconnect(parameters, result, proc, length)
}

func foreignTvfDisconnect(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	cache, ok := proc.GetSession().(process.ForeignConnCache)
	if !ok {
		return moerr.NewInvalidInput(proc.Ctx, "esql_tvf_disconnect/sql_tvf_disconnect requires an interactive session")
	}
	rs := vector.MustFunctionResult[bool](result)
	handleParam := vector.GenerateFunctionStrParameter(parameters[0])
	for i := uint64(0); i < uint64(length); i++ {
		h, hNull := handleParam.GetStrValue(i)
		if hNull {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}
		conn, removed := cache.RemoveForeignConn(string(h))
		if removed && conn != nil {
			_ = conn.Close()
		}
		if err := rs.Append(removed, false); err != nil {
			return err
		}
	}
	return nil
}
