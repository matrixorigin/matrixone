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

package function

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegexp2FunctionEntryPointsPreserveMaskAndNullSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	selectList := &FunctionSelectList{SelectList: []bool{true, true, true, false}}
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"ab", "ac", "ignored", "ab"}, []bool{false, false, true, false}),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{`a(?=b)`, `a(?=b)`, `a(?=b)`, `a(?=b)`}, nil),
	}

	for _, tc := range []struct {
		name string
		fn   fEvalFn
		want []bool
	}{
		{name: "regexp", fn: newOpBuiltInRegexp().builtInRegMatch, want: []bool{true, false, false, false}},
		{name: "not_regexp", fn: newOpBuiltInRegexp().builtInNotRegMatch, want: []bool{false, true, false, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseRun := NewFunctionTestCase(
				proc,
				inputs,
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, []bool{false, false, true, true}),
				tc.fn,
			).WithSelectList(selectList)
			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
		})
	}

	likeInputs := append(append([]FunctionTestInput(nil), inputs...),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"c", "c", "c", "c"}, nil))
	like := NewFunctionTestCase(
		proc,
		likeInputs,
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true, false, false, false}, []bool{false, false, true, true}),
		newOpBuiltInRegexp().builtInRegexpLike,
	).WithSelectList(selectList)
	succeed, info := like.Run()
	require.True(t, succeed, info)
}

func TestRegexp2FunctionEntryPointsValidateICUPatternBeforeNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	// A NULL subject does not suppress validation of a present ICU pattern.
	for _, tc := range []struct {
		name string
		fn   fEvalFn
		want types.Type
	}{
		{name: "instr", fn: newOpBuiltInRegexp().builtInRegexpInstr, want: types.T_int64.ToType()},
		{name: "substr", fn: newOpBuiltInRegexp().builtInRegexpSubstr, want: types.T_varchar.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseRun := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_varchar.ToType(), []string{""}, []bool{true}),
					NewFunctionTestInput(types.T_varchar.ToType(), []string{`(?=a)[`}, nil),
				},
				NewFunctionTestResult(tc.want, true, nil, nil),
				tc.fn,
			)
			succeed, info := caseRun.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestRegexp2SearchAndReplaceEntryPoints(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mask := &FunctionSelectList{SelectList: []bool{true, true, false}}

	instr := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"zab", "za", "ignored"}, []bool{false, false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`a(?=b)`, `a(?=b)`, `a(?=b)`}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
			NewFunctionTestInput(types.T_int8.ToType(), []int8{0, 0, 0}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"c", "c", "c"}, nil),
		},
		NewFunctionTestResult(types.T_int64.ToType(), false, []int64{2, 0, 0}, []bool{false, false, true}),
		newOpBuiltInRegexp().builtInRegexpInstr,
	).WithSelectList(mask)
	succeed, info := instr.Run()
	require.True(t, succeed, info)

	substr := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"zab", "za", "ignored"}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`a(?=b)`, `a(?=b)`, `a(?=b)`}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"c", "c", "c"}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"a", "", ""}, []bool{false, true, true}),
		newOpBuiltInRegexp().builtInRegexpSubstr,
	).WithSelectList(mask)
	succeed, info = substr.Run()
	require.True(t, succeed, info)

	replace := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"aa", "ba", "ignored"}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{`(a)\1`, `(a)\1`, `(a)\1`}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"<$0>", "<$0>", "<$0>"}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1}, nil),
			NewFunctionTestInput(types.T_varchar.ToType(), []string{"c", "c", "c"}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"<aa>", "ba", ""}, []bool{false, false, true}),
		newOpBuiltInRegexp().builtInRegexpReplace,
	).WithSelectList(mask)
	succeed, info = replace.Run()
	require.True(t, succeed, info)
}

func TestRegexp2ResourceAndOffsetContracts(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	matcher, err := rs.getRegexp2MatcherWithMatchType(`(?<word>a)\k<word>`, "", false)
	require.NoError(t, err)
	require.Equal(t, 1, matcher.NumSubexp())
	require.Equal(t, 1, matcher.SubexpIndex("word"))
	require.Equal(t, -1, matcher.SubexpIndex("missing"))

	prepared, err := matcher.forSubject("a😀")
	require.NoError(t, err)
	t.Cleanup(prepared.release)
	input, err := newRegexp2Input("a😀", false, 0)
	require.NoError(t, err)
	t.Cleanup(input.release)
	require.Equal(t, 1, input.runeIndexAtByte(1))
	require.Equal(t, 5, input.byteOffsetAtRune(2))
	input.release()
	input.release() // release is intentionally idempotent on an already-cleared input.
	prepared.release()
	prepared.release() // matcher release also tolerates repeated cleanup.

	before := regexp2ActiveSubjectBytes.Load()
	_, err = newRegexp2Input(strings.Repeat("x", 8), false, -1)
	require.Error(t, err)
	require.Equal(t, before, regexp2ActiveSubjectBytes.Load())
	_, err = (*regexp2Matcher)(nil).forSubject("x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil regular expression matcher")
	require.NoError(t, regexp2MatchError(nil))
	require.Error(t, regexp2MatchError(errors.New("ordinary matcher failure")))
	require.Error(t, regexp2MatchError(errors.New("match timeout")))
	require.Error(t, validateRegexp2CodeBudget(nil))
	_, err = regexp2EvaluationMemoryBudget(nil, 0, 0)
	require.Error(t, err)

	_, err = regexp2GraphemePatternForSubject(string([]byte{0xff}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "valid UTF-8")
	_, err = regexp2GraphemePatternForSubject(strings.Repeat("a", regexp2MaxGraphemeSubjectBytes+1))
	require.Error(t, err)

	// Exercise eviction of an ICU entry, in addition to the ordinary regexp
	// entry eviction covered by the existing cache tests.
	key := regexpCacheKey{pattern: "icu-only"}
	rs.mp = nil
	rs.icu = make(map[regexpCacheKey]*regexp2Matcher)
	rs.icu[key] = &regexp2Matcher{estimatedBytes: 128}
	rs.icuBytes = 128
	rs.evictRegexpCacheEntry()
	require.Empty(t, rs.icu)
	require.Zero(t, rs.icuBytes)

	require.Equal(t, `\99`, remapRegexp2Backreferences(`\99`, []int{0, 1}))
	require.Equal(t, `\1`, remapRegexp2Backreferences(`\1`, []int{0, 1}))
}

func TestRegexp2EvaluationCleanupOnDeadlineAndCallbackError(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	matcher, err := rs.getRegexp2MatcherWithMatchType(`a(?=b)`, "", false)
	require.NoError(t, err)

	before := regexp2ActiveSubjectBytes.Load()
	deadlineCallbackCalled := false
	_, err = rs.regexp2VisitMatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, "ab", 0, false, "", 0, time.Now().Add(-time.Second),
		func(start, end int) error {
			deadlineCallbackCalled = true
			return nil
		})
	require.Error(t, err)
	require.Contains(t, err.Error(), "timed out")
	require.False(t, deadlineCallbackCalled)
	require.Equal(t, before, regexp2ActiveSubjectBytes.Load())

	callbackErr := errors.New("test callback failure")
	_, err = rs.regexp2VisitMatchesAtOrAfterWithMatchTypeAndDeadline(
		matcher, "ab", 0, false, "", 0, time.Now().Add(time.Second),
		func(start, end int) error {
			return callbackErr
		})
	require.ErrorIs(t, err, callbackErr)
	require.Equal(t, before, regexp2ActiveSubjectBytes.Load())
}

func TestRegexp2AdmissionExhaustionAndRecovery(t *testing.T) {
	before := regexp2ActiveSubjectBytes.Load()
	start := make(chan struct{})
	release := make(chan struct{})
	type admissionResult struct {
		reserved int64
		err      error
	}
	results := make(chan admissionResult, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			reserved, err := acquireRegexp2SubjectBudget(0, 40<<20)
			results <- admissionResult{reserved: reserved, err: err}
			if err == nil {
				<-release
				regexp2ActiveSubjectBytes.Add(-reserved)
			}
		}()
	}
	close(start)

	successes := 0
	failures := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil {
			successes++
		} else {
			failures++
			require.Contains(t, result.err.Error(), "active memory budget")
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, failures)
	close(release)
	wg.Wait()
	require.Equal(t, before, regexp2ActiveSubjectBytes.Load())
}
