// Copyright 2026 Matrix Origin
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

package frontend

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
)

// TestBuiltinFailureIsStillAPlainString is the compatibility regression, and
// it is written at the LANGUAGE level on purpose: containment, equality,
// ordering, len, indexing, slicing and hashing are resolved by starlark on the
// value's concrete type and never call a method by name, so a Go-level test
// that calls String() or Attr() proves nothing about them.
//
// Every expression is evaluated against the value a failing builtin hands back
// and must behave exactly as it does for the equivalent plain string.
func TestBuiltinFailureIsStillAPlainString(t *testing.T) {
	si := &starlarkInterpreter{}
	v, err := si.moQuote(nil, starlark.NewBuiltin("mo.quote", si.moQuote),
		starlark.Tuple{starlark.MakeInt(1)}, nil)
	require.NoError(t, err)
	okSlot := v.(*starlark.List).Index(1)

	// it IS a starlark.String, not something that merely prints like one --
	// which is what makes every operation below work by construction
	msg, isString := okSlot.(starlark.String)
	require.True(t, isString, "the ok slot must stay a plain string, got %s", okSlot.Type())
	require.NotEmpty(t, string(msg))

	for _, expr := range []string{
		`"got int" in err`, `err in ("prefix " + MSG)`,
		`err == MSG`, `err != MSG`, `err == "something else"`,
		`sorted([err, "AAA"])`, `{err: 1}[MSG]`, `hash(err) == hash(MSG)`,
		`len(err)`, `err[0]`, `err[-1]`, `err[0:9]`, `err[::2]`,
		`[c for c in err.elems()][0]`,
		`err + "!"`, `"x: " + err`, `"%s" % err`, `"{}".format(err)`,
		`str(err)`, `repr(err)`, `type(err)`, `bool(err)`,
		`err.startswith("quote")`, `err.upper()`, `err.split(" ")[0]`,
	} {
		env := starlark.StringDict{"err": okSlot, "MSG": msg}
		got, gotErr := starlark.Eval(&starlark.Thread{}, "compat.star", expr, env)

		env2 := starlark.StringDict{"err": starlark.String(string(msg)), "MSG": msg}
		want, wantErr := starlark.Eval(&starlark.Thread{}, "compat.star", expr, env2)

		require.Equal(t, wantErr == nil, gotErr == nil, "%s", expr)
		if wantErr == nil {
			require.Equal(t, want.String(), got.String(), "%s", expr)
		}
	}
}

// TestErrnoReportsTheLastFailure covers the codes, which is what the string
// cannot carry: they come from mo.errno() / mo.sqlstate() instead.
func TestErrnoReportsTheLastFailure(t *testing.T) {
	si := &starlarkInterpreter{}
	errno := func() starlark.Value {
		v, err := si.moErrno(nil, starlark.NewBuiltin("mo.errno", si.moErrno), nil, nil)
		require.NoError(t, err)
		return v
	}
	sqlstate := func() starlark.Value {
		v, err := si.moSqlstate(nil, starlark.NewBuiltin("mo.sqlstate", si.moSqlstate), nil, nil)
		require.NoError(t, err)
		return v
	}

	// nothing has failed yet
	require.Equal(t, starlark.MakeInt(0), errno())
	require.Equal(t, starlark.String(""), sqlstate())

	// a moerr failure reports its class
	si.failed(moerr.NewDuplicateEntryNoCtx("1", "a"))
	require.Equal(t, starlark.MakeInt(1062), errno())
	require.Equal(t, starlark.String("23000"), sqlstate())

	// reporting does not consume: asking twice gives the same answer, or a
	// procedure could not read both the number and the state
	require.Equal(t, starlark.MakeInt(1062), errno())
	require.Equal(t, starlark.String("23000"), sqlstate())

	// a failure that is not a moerr has no number to borrow
	si.beginCall()
	si.failed(errors.New("argument unpacking failed"))
	require.Equal(t, starlark.MakeInt(0), errno())
	require.Equal(t, starlark.String(""), sqlstate())

	// and the NEXT call clears the record, so a success never reports the
	// previous failure's code
	si.failed(moerr.NewDuplicateEntryNoCtx("1", "a"))
	require.Equal(t, starlark.MakeInt(1062), errno())
	si.beginCall()
	require.Equal(t, starlark.MakeInt(0), errno())
	require.Equal(t, starlark.String(""), sqlstate())
}

// TestEveryFailableBuiltinResetsAndRecords: the guarantee is per CALL, so each
// builtin that can fail has to clear the record on entry. A builtin that
// forgot would let mo.errno() answer for some earlier statement.
func TestEveryFailableBuiltinResetsAndRecords(t *testing.T) {
	si := &starlarkInterpreter{}
	notAString := starlark.Tuple{starlark.MakeInt(1)}

	for _, c := range []struct {
		name string
		fn   func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error)
	}{
		{"mo.sql", si.moSql}, {"mo.jq", si.moJq}, {"mo.quote", si.moQuote},
		{"mo.getvar", si.moGetVar}, {"mo.setvar", si.moSetVar},
		{"mo.llm_connect", si.moLlmConnect}, {"mo.llm_chat", si.moLlmChat},
	} {
		// a stale duplicate-key code from an earlier statement
		si.lastErr = lastFailure{code: 1062, sqlstate: "23000", set: true}

		v, err := c.fn(nil, starlark.NewBuiltin(c.name, c.fn), notAString, nil)
		require.NoError(t, err, "%s must not abort the script", c.name)

		list, ok := v.(*starlark.List)
		require.True(t, ok, "%s must return the [result, ok] pair", c.name)
		require.Equal(t, 2, list.Len())
		require.Equal(t, starlark.None, list.Index(0), "%s", c.name)
		_, isString := list.Index(1).(starlark.String)
		require.True(t, isString, "%s must report the failure as a plain string", c.name)

		require.Equal(t, uint16(0), si.lastErr.code,
			"%s must clear the previous call's code", c.name)
		require.Equal(t, "", si.lastErr.sqlstate, "%s", c.name)
	}
}
