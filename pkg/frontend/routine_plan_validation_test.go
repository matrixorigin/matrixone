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
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/stretchr/testify/require"
)

type routinePlanValidationExec struct {
	BackgroundExec
	resultSets          []interface{}
	catalogResultSets   []interface{}
	namespaceResultSets []interface{}
	failOn              string
	statements          []string
}

func (e *routinePlanValidationExec) Close() {}

func (e *routinePlanValidationExec) Exec(_ context.Context, statement string) error {
	e.statements = append(e.statements, statement)
	if e.failOn != "" && strings.Contains(statement, e.failOn) {
		return errors.New("injected routine catalog read failure")
	}
	if strings.Contains(statement, "select f.function_id") {
		e.resultSets = e.catalogResultSets
	} else if strings.Contains(statement, "select selected.function_id") {
		e.resultSets = e.namespaceResultSets
	}
	return nil
}

func (e *routinePlanValidationExec) GetExecResultSet() []interface{} {
	return e.resultSets
}

func (e *routinePlanValidationExec) ClearExecResultSet() {
	e.resultSets = nil
}

type routinePlanValidationSession struct {
	FeSession
	exec BackgroundExec
}

func (s *routinePlanValidationSession) GetAccountId() uint32 { return 9 }

func (s *routinePlanValidationSession) GetTxnCompileCtx() *TxnCompilerContext { return nil }

func (s *routinePlanValidationSession) GetBackgroundExec(context.Context, ...*BackgroundExecOption) BackgroundExec {
	return s.exec
}

func routinePlanCatalogResultSet(values []interface{}) *MysqlResultSet {
	result := &MysqlResultSet{}
	for range values {
		result.AddColumn(&MysqlColumn{})
	}
	result.AddRow(values)
	return result
}

func emptyRoutineNamespaceResultSet() *MysqlResultSet {
	result := &MysqlResultSet{}
	for range 16 {
		result.AddColumn(&MysqlColumn{})
	}
	return result
}

func testRoutinePlanDependency() *planpb.RoutinePlanDependency {
	return &planpb.RoutinePlanDependency{
		FunctionRef: &planpb.FunctionRef{
			FunctionId:       41,
			Revision:         7,
			NamespaceVersion: 12,
			AccountId:        9,
			DatabaseId:       8,
		},
		Language:              udf.LanguagePython,
		ContractVersion:       udf.PythonPlanContractVersion,
		NamespaceFingerprint:  strings.Repeat("d", 64),
		DefinitionFingerprint: strings.Repeat("a", 64),
		ArtifactDigest:        strings.Repeat("b", 64),
		EnvironmentDigest:     strings.Repeat("c", 64),
		Volatility:            "VOLATILE",
		NullPolicy:            udf.NullCallHandler,
		MayError:              true,
		SecurityMode:          "INVOKER",
		Leakproof:             false,
	}
}

func testRoutinePlanState() routinePlanCatalogState {
	return routinePlanCatalogState{
		namespaceFingerprint: strings.Repeat("d", 64),
		activeRevision:       7,
		namespaceVersion:     12,
		databaseID:           8,
		revision:             7,
		language:             udf.LanguagePython,
		volatility:           "VOLATILE",
		nullPolicy:           udf.NullCallHandler,
		fingerprint:          strings.Repeat("a", 64),
		artifactDigest:       strings.Repeat("b", 64),
		environmentDigest:    strings.Repeat("c", 64),
		securityType:         "INVOKER",
	}
}

func TestRoutinePlanDependenciesChangedRequiresExactPublication(t *testing.T) {
	dependency := testRoutinePlanDependency()
	state := testRoutinePlanState()

	require.False(t, routinePlanDependenciesChanged(
		[]*planpb.RoutinePlanDependency{dependency},
		map[uint64]routinePlanCatalogState{41: state},
	))

	cases := []struct {
		name   string
		mutate func(*routinePlanCatalogState)
	}{
		{name: "active revision", mutate: func(s *routinePlanCatalogState) {
			s.activeRevision++
		}},
		{name: "namespace", mutate: func(s *routinePlanCatalogState) {
			s.namespaceVersion++
		}},
		{name: "database", mutate: func(s *routinePlanCatalogState) {
			s.databaseID++
		}},
		{name: "revision", mutate: func(s *routinePlanCatalogState) {
			s.revision++
		}},
		{name: "fingerprint", mutate: func(s *routinePlanCatalogState) {
			s.fingerprint = strings.Repeat("d", 64)
		}},
		{name: "artifact", mutate: func(s *routinePlanCatalogState) {
			s.artifactDigest = strings.Repeat("d", 64)
		}},
		{name: "environment", mutate: func(s *routinePlanCatalogState) {
			s.environmentDigest = strings.Repeat("d", 64)
		}},
		{name: "security", mutate: func(s *routinePlanCatalogState) {
			s.securityType = "DEFINER"
		}},
		{name: "language", mutate: func(s *routinePlanCatalogState) {
			s.language = "sql"
		}},
		{name: "volatility", mutate: func(s *routinePlanCatalogState) {
			s.volatility = "IMMUTABLE"
		}},
		{name: "missing", mutate: func(s *routinePlanCatalogState) {
			s.activeRevision = 0
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changedState := state
			tc.mutate(&changedState)
			states := map[uint64]routinePlanCatalogState{41: changedState}
			if tc.name == "missing" {
				states = map[uint64]routinePlanCatalogState{}
			}
			require.True(t, routinePlanDependenciesChanged(
				[]*planpb.RoutinePlanDependency{dependency}, states))
		})
	}
}

func TestValidateRoutinePlanDependencyShapeRejectsCrossAccountAndUnknownContracts(t *testing.T) {
	require.NoError(t, validateRoutinePlanDependencyShape(testRoutinePlanDependency(), 9))

	cases := []struct {
		name   string
		mutate func(*planpb.RoutinePlanDependency)
	}{
		{name: "cross account", mutate: func(d *planpb.RoutinePlanDependency) {
			d.FunctionRef.AccountId = 10
		}},
		{name: "missing database", mutate: func(d *planpb.RoutinePlanDependency) {
			d.FunctionRef.DatabaseId = 0
		}},
		{name: "unknown contract", mutate: func(d *planpb.RoutinePlanDependency) {
			d.ContractVersion++
		}},
		{name: "missing integrity", mutate: func(d *planpb.RoutinePlanDependency) {
			d.ArtifactDigest = ""
		}},
		{name: "malformed digest", mutate: func(d *planpb.RoutinePlanDependency) {
			d.DefinitionFingerprint = "definition-fingerprint"
		}},
		{name: "missing identity", mutate: func(d *planpb.RoutinePlanDependency) {
			d.FunctionRef = nil
		}},
		{name: "noncanonical language", mutate: func(d *planpb.RoutinePlanDependency) {
			d.Language = "PYTHON"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dependency := testRoutinePlanDependency()
			tc.mutate(dependency)
			require.ErrorContains(t,
				validateRoutinePlanDependencyShape(dependency, 9),
				"UNSUPPORTED_ROUTINE_VERSION")
		})
	}
}

func TestRoutinePlanDependenciesChangedRequiresCanonicalLanguage(t *testing.T) {
	dependency := testRoutinePlanDependency()
	dependency.Language = "PYTHON"

	require.True(t, routinePlanDependenciesChanged(
		[]*planpb.RoutinePlanDependency{dependency},
		map[uint64]routinePlanCatalogState{41: testRoutinePlanState()},
	))
}

func TestRoutinePlanDependenciesIncludesBackgroundQueries(t *testing.T) {
	dependency := testRoutinePlanDependency()
	plan := &planpb.Plan{
		Plan: &planpb.Plan_Query{Query: &planpb.Query{
			RoutineDependencies: []*planpb.RoutinePlanDependency{dependency},
			BackgroundQueries: []*planpb.Query{{
				RoutineDependencies: []*planpb.RoutinePlanDependency{testRoutinePlanDependency()},
			}},
		}},
	}
	dependencies, err := routinePlanDependencies(plan)
	require.NoError(t, err)
	require.Len(t, dependencies, 2)
	require.Same(t, dependency, dependencies[0])
	require.Equal(t, dependency, dependencies[0])
}

func TestRoutinePlanDependenciesRejectsUnboundedQueryGraph(t *testing.T) {
	root := &planpb.Query{}
	current := root
	for depth := 0; depth < maxRoutinePlanQueryDepth+1; depth++ {
		next := &planpb.Query{}
		current.BackgroundQueries = []*planpb.Query{next}
		current = next
	}

	_, err := routinePlanDependencies(&planpb.Plan{
		Plan: &planpb.Plan_Query{Query: root},
	})
	require.ErrorContains(t, err, "query nesting exceeds")
}

func TestRoutinePlanDependenciesRejectsOversizedClosureAndCycles(t *testing.T) {
	root := &planpb.Query{}
	root.RoutineDependencies = make([]*planpb.RoutinePlanDependency, maxRoutinePlanDependencies+1)
	_, err := routinePlanDependencies(&planpb.Plan{
		Plan: &planpb.Plan_Query{Query: root},
	})
	require.ErrorContains(t, err, "dependency count exceeds")

	oversized := testRoutinePlanDependency()
	oversized.DefinitionFingerprint = strings.Repeat("a", maxRoutinePlanDependencyBytes)
	_, err = routinePlanDependencies(&planpb.Plan{
		Plan: &planpb.Plan_Query{Query: &planpb.Query{
			RoutineDependencies: []*planpb.RoutinePlanDependency{oversized},
		}},
	})
	require.ErrorContains(t, err, "dependency bytes exceed")

	cycle := &planpb.Query{}
	cycle.BackgroundQueries = []*planpb.Query{cycle}
	_, err = routinePlanDependencies(&planpb.Plan{
		Plan: &planpb.Plan_Query{Query: cycle},
	})
	require.ErrorContains(t, err, "graph contains a cycle")
}

func TestCachedRoutinePlanWithDependencyRequiresCatalogSession(t *testing.T) {
	plan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		RoutineDependencies: []*planpb.RoutinePlanDependency{testRoutinePlanDependency()},
	}}}
	plain := &cachedPlan{plans: []*sqlplan.Plan{{Plan: &planpb.Plan_Query{Query: &planpb.Query{}}}}}
	withRoutine := &cachedPlan{plans: []*sqlplan.Plan{plan}}
	require.True(t, cachedRoutinePlanDependenciesCurrent(nil, plain))
	require.False(t, cachedRoutinePlanDependenciesCurrent(nil, withRoutine))
}

func TestValidateRoutinePlanDependenciesUsesCatalogTransactionAndFailsClosed(t *testing.T) {
	ctx := context.Background()
	plainPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{}}}
	changed, err := validateRoutinePlanDependencies(ctx, nil, plainPlan)
	require.NoError(t, err)
	require.False(t, changed)

	dependency := testRoutinePlanDependency()
	plan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		RoutineDependencies: []*planpb.RoutinePlanDependency{dependency},
	}}}
	_, err = validateRoutinePlanDependencies(ctx, nil, plan)
	require.ErrorContains(t, err, "has no session")

	incomplete := testRoutinePlanDependency()
	incomplete.NamespaceFingerprint = ""
	incompletePlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		RoutineDependencies: []*planpb.RoutinePlanDependency{incomplete},
	}}}
	changed, err = validateRoutinePlanDependencies(ctx, &routinePlanValidationSession{}, incompletePlan)
	require.NoError(t, err)
	require.True(t, changed, "an incomplete namespace fence must invalidate without a catalog read")

	exec := &routinePlanValidationExec{}
	session := &routinePlanValidationSession{exec: exec}
	changed, err = validateRoutinePlanDependencies(ctx, session, plan)
	require.NoError(t, err)
	require.True(t, changed, "a missing active catalog revision invalidates the prepared plan")
	require.Len(t, exec.statements, 3)
	require.Equal(t, "begin;", exec.statements[0])
	require.Contains(t, exec.statements[1], "where f.function_id in (41)")
	require.Equal(t, "commit;", exec.statements[2])

	// A present but damaged immutable revision is fully decoded and invalidates
	// the cached plan; namespace state is still read in the same transaction.
	row := []interface{}{
		uint64(41), uint64(7), uint64(12), uint64(8), uint64(7),
		"python", "VOLATILE", strings.Repeat("a", 64), strings.Repeat("b", 64),
		strings.Repeat("c", 64), "[]", "{}", int64(udf.PythonSignatureKeySchemaVersion),
		strings.Repeat("d", 64), "damaged body", "[]", "bigint",
		int64(udf.PythonDefinitionSchemaVersion), udf.PythonABIContract,
		udf.PythonAdapterVersion, udf.PythonSDKVersion, udf.NullCallHandler,
		"INVOKER", "INVOKER",
	}
	fullExec := &routinePlanValidationExec{
		catalogResultSets:   []interface{}{routinePlanCatalogResultSet(row)},
		namespaceResultSets: []interface{}{emptyRoutineNamespaceResultSet()},
	}
	changed, err = validateRoutinePlanDependencies(ctx,
		&routinePlanValidationSession{exec: fullExec}, plan)
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, fullExec.statements, 4)
	require.Contains(t, fullExec.statements[2], "select selected.function_id")
	require.Equal(t, "commit;", fullExec.statements[3])

	for _, tc := range []struct {
		name              string
		catalogResultSets []interface{}
		failOn            string
		want              string
		rollback          bool
	}{
		{name: "transaction begin error", failOn: "begin;", want: "could not start"},
		{name: "catalog query error", failOn: "mo_user_defined_function", want: "validation failed", rollback: true},
		{name: "malformed result set", catalogResultSets: []interface{}{struct{}{}}, want: "not the type of result set", rollback: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failingExec := &routinePlanValidationExec{catalogResultSets: tc.catalogResultSets, failOn: tc.failOn}
			changed, err := validateRoutinePlanDependencies(ctx,
				&routinePlanValidationSession{exec: failingExec}, plan)
			require.False(t, changed)
			require.ErrorContains(t, err, tc.want)
			lastStatement := failingExec.statements[len(failingExec.statements)-1]
			if tc.rollback {
				require.Equal(t, "rollback;", lastStatement)
			} else {
				require.Equal(t, "begin;", lastStatement)
			}
		})
	}
}

func TestExactlyOneCatalogResultSetRequiresOneCompleteResultSet(t *testing.T) {
	ctx := context.Background()
	_, err := exactlyOneCatalogResultSet(ctx, nil, "routine dependency catalog")
	require.ErrorContains(t, err, "expected exactly one")

	_, err = exactlyOneCatalogResultSet(ctx, []ExecResult{nil, nil}, "routine dependency catalog")
	require.ErrorContains(t, err, "returned 2 result sets")

	result := singleInt64Result("function_id", 41)
	got, err := exactlyOneCatalogResultSet(ctx, []ExecResult{result}, "routine dependency catalog")
	require.NoError(t, err)
	require.Same(t, result, got)
}

func TestRoutinePlanCatalogIdentityRejectsTamperedRevisionBody(t *testing.T) {
	raw := pythonCatalogTestBody(t, types.T_int64)
	body, err := function.DecodePythonRoutineBody(raw)
	require.NoError(t, err)
	input, output, signature, err := function.PythonSignatureMetadata(body.ArgTypes, body.ReturnType)
	require.NoError(t, err)
	fingerprint, err := function.PythonRoutineBodyFingerprint(body)
	require.NoError(t, err)

	valid := func(candidate string) bool {
		return routinePlanCatalogIdentityValid(
			candidate, fingerprint, udf.PythonSignatureKeySchemaVersion,
			input, output, signature, input, "bigint",
			udf.PythonDefinitionSchemaVersion, udf.PythonABIContract,
			udf.PythonAdapterVersion, udf.PythonSDKVersion, udf.NullCallHandler,
		)
	}
	require.True(t, valid(raw))

	body.Source = "def f(ctx, value): return value + 1"
	tampered, err := json.Marshal(body)
	require.NoError(t, err)
	require.False(t, valid(string(tampered)), "retaining an old revision fingerprint must invalidate the cached plan")

	require.False(t, routinePlanCatalogIdentityValid(
		raw, fingerprint, udf.PythonSignatureKeySchemaVersion,
		input, output, signature, input+" ", "bigint",
		udf.PythonDefinitionSchemaVersion, udf.PythonABIContract,
		udf.PythonAdapterVersion, udf.PythonSDKVersion, udf.NullCallHandler,
	))
}

func TestRoutinePlanCatalogIdentityAcceptsAndRejectsSQLRevision(t *testing.T) {
	const (
		body     = "select value + 1"
		argTypes = "[\"bigint\"]"
		retType  = "bigint"
	)
	fingerprint, err := function.SQLRoutineFingerprint(body, argTypes, retType)
	require.NoError(t, err)
	valid := func(candidateBody, candidateArgs, candidateReturn, candidateFingerprint string) bool {
		return routinePlanCatalogIdentityValidForLanguage(
			udf.LanguageSQL,
			candidateBody, candidateFingerprint, 0, "", "", "",
			candidateArgs, candidateReturn, udf.SQLDefinitionSchemaVersion,
			"", "", "", udf.NullCallHandler,
		)
	}
	require.True(t, valid(body, argTypes, retType, fingerprint))
	require.False(t, valid("select value + 2", argTypes, retType, fingerprint))
	require.False(t, valid(body, argTypes, "int", fingerprint))
	require.False(t, valid(body, argTypes, retType, strings.Repeat("a", 64)))
	require.False(t, routinePlanCatalogIdentityValidForLanguage(
		"lua", body, fingerprint, 0, "", "", "", argTypes, retType,
		udf.SQLDefinitionSchemaVersion, "", "", "", udf.NullCallHandler,
	))
	require.False(t, routinePlanCatalogIdentityValidForLanguage(
		"PYTHON", body, fingerprint, udf.PythonSignatureKeySchemaVersion,
		"", "", "", argTypes, retType, udf.PythonDefinitionSchemaVersion,
		udf.PythonABIContract, udf.PythonAdapterVersion, udf.PythonSDKVersion,
		udf.NullCallHandler,
	))
}

func TestRoutinePlanDependenciesVisitsSharedQueriesOnce(t *testing.T) {
	dependency := testRoutinePlanDependency()
	query := &planpb.Query{RoutineDependencies: []*planpb.RoutinePlanDependency{dependency}}
	for depth := 0; depth < 12; depth++ {
		query = &planpb.Query{BackgroundQueries: []*planpb.Query{query, query}}
	}
	dependencies, err := routinePlanDependencies(&planpb.Plan{Plan: &planpb.Plan_Query{Query: query}})
	require.NoError(t, err)
	require.Len(t, dependencies, 1)
	require.Same(t, dependency, dependencies[0])
}
