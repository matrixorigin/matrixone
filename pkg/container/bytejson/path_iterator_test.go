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

package bytejson

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The reference evaluator intentionally operates on encoding/json values and
// its own path-step type. It does not call ByteJson.Query, Unnest, or any
// iterator helper, so a shared eager implementation cannot make these tests
// pass accidentally.
type referenceStepKind uint8

const (
	referenceKey referenceStepKind = iota + 1
	referenceArrayWildcard
	referenceObjectWildcard
	referenceArrayRange
	referenceRecursive
	referenceArrayIndex
)

type referenceStep struct {
	kind       referenceStepKind
	key        string
	start, end int
}

func referenceMatches(root any, steps []referenceStep) []any {
	matches := make([]any, 0)
	var walk func(any, int)
	walk = func(value any, stepIndex int) {
		if stepIndex == len(steps) {
			matches = append(matches, value)
			return
		}
		step := steps[stepIndex]
		switch step.kind {
		case referenceKey:
			object, ok := value.(map[string]any)
			if !ok {
				return
			}
			child, ok := object[step.key]
			if ok {
				walk(child, stepIndex+1)
			}
		case referenceArrayWildcard:
			array, ok := value.([]any)
			if !ok {
				return
			}
			for _, child := range array {
				walk(child, stepIndex+1)
			}
		case referenceObjectWildcard:
			object, ok := value.(map[string]any)
			if !ok {
				return
			}
			keys := make([]string, 0, len(object))
			for key := range object {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				walk(object[key], stepIndex+1)
			}
		case referenceArrayRange:
			array, ok := value.([]any)
			if !ok {
				return
			}
			start, end := step.start, step.end
			if start < 0 {
				start = 0
			}
			if end >= len(array) {
				end = len(array) - 1
			}
			for index := start; index <= end; index++ {
				walk(array[index], stepIndex+1)
			}
		case referenceArrayIndex:
			if array, ok := value.([]any); ok {
				if step.start >= 0 && step.start < len(array) {
					walk(array[step.start], stepIndex+1)
				}
			} else if step.start == 0 {
				// ByteJson follows the existing JSON path contract and
				// autowraps a scalar for index zero.
				walk(value, stepIndex+1)
			}
		case referenceRecursive:
			// JSON path recursive descent tries the suffix at this value first,
			// then visits children in document order. Objects are sorted because
			// ByteJson's canonical binary representation sorts object keys.
			walk(value, stepIndex+1)
			switch composite := value.(type) {
			case []any:
				for _, child := range composite {
					walk(child, stepIndex)
				}
			case map[string]any:
				keys := make([]string, 0, len(composite))
				for key := range composite {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					walk(composite[key], stepIndex)
				}
			}
		}
	}
	walk(root, 0)
	return matches
}

func decodeReferenceDocument(t *testing.T, document string) any {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(document))
	decoder.UseNumber()
	var value any
	require.NoError(t, decoder.Decode(&value))
	return value
}

func collectIteratorMatches(t *testing.T, document, pathText string) []string {
	t.Helper()
	root, err := ParseFromString(document)
	require.NoError(t, err)
	path, err := ParseJsonPath(pathText)
	require.NoError(t, err)
	iterator := NewPathIterator(root, &path)
	defer iterator.Close()

	matches := make([]string, 0)
	for {
		value, matched, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		if !matched {
			return matches
		}
		encoded, marshalErr := value.MarshalJSON()
		require.NoError(t, marshalErr)
		matches = append(matches, normalizeJSON(t, encoded))
	}
}

func encodeReferenceMatches(t *testing.T, matches []any) []string {
	t.Helper()
	encoded := make([]string, 0, len(matches))
	for _, value := range matches {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		encoded = append(encoded, normalizeJSON(t, data))
	}
	return encoded
}

func normalizeJSON(t *testing.T, data []byte) string {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	require.NoError(t, decoder.Decode(&value))
	normalized, err := json.Marshal(value)
	require.NoError(t, err)
	return string(normalized)
}

func TestPathIteratorMatchesIndependentReference(t *testing.T) {
	cases := []struct {
		name     string
		document string
		path     string
		steps    []referenceStep
	}{
		{
			name:     "root",
			document: `{"value":1}`,
			path:     `$`,
		},
		{
			name:     "nested key",
			document: `{"nested":{"value":7}}`,
			path:     `$.nested.value`,
			steps: []referenceStep{
				{kind: referenceKey, key: "nested"},
				{kind: referenceKey, key: "value"},
			},
		},
		{
			name:     "array range",
			document: `[10,20,30,40]`,
			path:     `$[1 to 2]`,
			steps:    []referenceStep{{kind: referenceArrayRange, start: 1, end: 2}},
		},
		{
			name:     "scalar index zero autowrap",
			document: `"scalar"`,
			path:     `$[0]`,
			steps:    []referenceStep{{kind: referenceArrayIndex, start: 0}},
		},
		{
			name:     "empty array",
			document: `[]`,
			path:     `$[*]`,
			steps:    []referenceStep{{kind: referenceArrayWildcard}},
		},
		{
			name:     "array index missing",
			document: `[1]`,
			path:     `$[2]`,
			steps:    []referenceStep{{kind: referenceArrayIndex, start: 2}},
		},
		{
			name:     "nested wildcards",
			document: `[{"a":[1,2]},{"a":[3]},{"a":[]}]`,
			path:     `$[*].a[*]`,
			steps: []referenceStep{
				{kind: referenceArrayWildcard},
				{kind: referenceKey, key: "a"},
				{kind: referenceArrayWildcard},
			},
		},
		{
			name:     "object wildcard",
			document: `{"z":3,"a":1}`,
			path:     `$.*`,
			steps:    []referenceStep{{kind: referenceObjectWildcard}},
		},
		{
			name:     "recursive descent",
			document: `{"a":1,"nested":{"a":2,"deep":{"a":3}}}`,
			path:     `$**.a`,
			steps: []referenceStep{
				{kind: referenceRecursive},
				{kind: referenceKey, key: "a"},
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			want := encodeReferenceMatches(t, referenceMatches(decodeReferenceDocument(t, test.document), test.steps))
			got := collectIteratorMatches(t, test.document, test.path)
			require.Equal(t, want, got)
		})
	}
}

func TestPathIteratorDistinguishesMissingAndJSONNull(t *testing.T) {
	document := `{"present":null}`
	require.Equal(t, []string{"null"}, collectIteratorMatches(t, document, `$.present`))
	require.Empty(t, collectIteratorMatches(t, document, `$.missing`))
}

func TestPathIteratorResetStartsFreshAndCloseReleasesCursor(t *testing.T) {
	first, err := ParseFromString(`[1,2]`)
	require.NoError(t, err)
	second, err := ParseFromString(`[3,4]`)
	require.NoError(t, err)
	path, err := ParseJsonPath(`$[*]`)
	require.NoError(t, err)

	iterator := NewPathIterator(first, &path)
	value, matched, err := iterator.Next()
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "1", value.String())

	iterator.Reset(second, &path)
	require.Equal(t, []string{"3", "4"}, collectRemainingMatches(t, iterator))
	iterator.Close()
	_, matched, err = iterator.Next()
	require.NoError(t, err)
	require.False(t, matched)

	iterator.Reset(first, &path)
	require.Equal(t, []string{"1", "2"}, collectRemainingMatches(t, iterator))
	iterator.Close()
}

func TestPathIteratorClearsPoppedBorrowedFramesAcrossReset(t *testing.T) {
	first, err := ParseFromString(`[[[1]]]`)
	require.NoError(t, err)
	deepPath, err := ParseJsonPath(`$[*][*][*]`)
	require.NoError(t, err)
	iterator := NewPathIterator(first, &deepPath)

	value, matched, err := iterator.Next()
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "1", value.String())
	_, matched, err = iterator.Next()
	require.NoError(t, err)
	require.False(t, matched)
	require.Greater(t, cap(iterator.stack), 0)

	retained := iterator.stack[:cap(iterator.stack)]
	for i, frame := range retained {
		require.Nil(t, frame.value.Data, "popped frame %d still retains source data", i)
	}

	second, err := ParseFromString(`2`)
	require.NoError(t, err)
	rootPath, err := ParseJsonPath(`$`)
	require.NoError(t, err)
	iterator.Reset(second, &rootPath)
	require.Equal(t, second.Data, iterator.stack[0].value.Data)
	for i, frame := range iterator.stack[1:cap(iterator.stack)] {
		require.Nil(t, frame.value.Data, "reset frame %d still retains prior source data", i+1)
	}
	iterator.Close()
}

func collectRemainingMatches(t *testing.T, iterator *PathIterator) []string {
	t.Helper()
	var matches []string
	for {
		value, matched, err := iterator.Next()
		require.NoError(t, err)
		if !matched {
			return matches
		}
		matches = append(matches, value.String())
	}
}

func TestPathIteratorCancellationLeavesCursorResumable(t *testing.T) {
	root, err := ParseFromString(`[1,2]`)
	require.NoError(t, err)
	path, err := ParseJsonPath(`$[*]`)
	require.NoError(t, err)
	iterator := NewPathIterator(root, &path)
	defer iterator.Close()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, matched, err := iterator.NextContext(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, matched)

	deadline, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	value, matched, err := iterator.NextContext(deadline)
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "1", value.String())
}

func TestPathIteratorDoesNotCollectWholeWildcard(t *testing.T) {
	const count = 10_000
	var document strings.Builder
	document.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			document.WriteByte(',')
		}
		document.WriteString(strconv.Itoa(i))
	}
	document.WriteByte(']')

	root, err := ParseFromString(document.String())
	require.NoError(t, err)
	path, err := ParseJsonPath(`$[*]`)
	require.NoError(t, err)
	iterator := NewPathIterator(root, &path)
	defer iterator.Close()
	value, matched, err := iterator.Next()
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "0", value.String())
	// A wildcard keeps only its cursor and the currently yielded child. A
	// slice of all matches would grow with count and fail this bound.
	require.Less(t, cap(iterator.stack), 128)
	require.LessOrEqual(t, len(iterator.stack), 2)
}

func TestPathIteratorRejectsInvalidDocumentPathAndDefault(t *testing.T) {
	_, err := ParseFromString(`{"x":`)
	require.Error(t, err, "invalid source document")
	_, err = ParseJsonPath(`$.`)
	require.Error(t, err, "invalid path")
	_, err = ParseFromString(`{`)
	require.Error(t, err, "a JSON_TABLE default literal uses the same strict JSON parser")

	root, err := ParseFromString(`1`)
	require.NoError(t, err)
	iterator := NewPathIterator(root, nil)
	defer iterator.Close()
	_, matched, err := iterator.Next()
	require.Error(t, err)
	require.False(t, matched)

	malformed := NewPathIterator(ByteJson{Type: TpCodeArray, Data: []byte{1}}, func() *Path {
		path, pathErr := ParseJsonPath(`$[*]`)
		require.NoError(t, pathErr)
		return &path
	}())
	defer malformed.Close()
	_, matched, err = malformed.Next()
	require.Error(t, err)
	require.False(t, matched)
}

func TestPathIteratorRejectsSelfReferentialContainer(t *testing.T) {
	data := make([]byte, headerSize+valEntrySize)
	binary.LittleEndian.PutUint32(data, 1)
	binary.LittleEndian.PutUint32(data[docSizeOff:], uint32(len(data)))
	data[headerSize] = byte(TpCodeArray)
	binary.LittleEndian.PutUint32(data[headerSize+valTypeSize:], 0)

	path, err := ParseJsonPath(`$**.a`)
	require.NoError(t, err)
	iterator := NewPathIterator(ByteJson{Type: TpCodeArray, Data: data}, &path)
	defer iterator.Close()
	_, matched, err := iterator.Next()
	require.Error(t, err)
	require.False(t, matched)
}

func TestPathIteratorAliasAndErrorTextAreStable(t *testing.T) {
	root, err := ParseFromString(`1`)
	require.NoError(t, err)
	iterator := NewPathMatchIterator(root, nil)
	defer iterator.Close()
	var alias *PathMatchIterator = iterator
	require.NotNil(t, alias)
	_, _, err = iterator.Next()
	require.ErrorContains(t, err, "requires a path")
}

func TestPathIteratorNilReceiverAndBoundarySteps(t *testing.T) {
	var nilIterator *PathIterator
	nilIterator.Reset(ByteJson{}, nil)
	nilIterator.Close()
	value, matched, err := nilIterator.NextContext(nil)
	require.NoError(t, err)
	require.False(t, matched)
	require.Equal(t, ByteJson{}, value)

	var empty PathIterator
	empty.popFrame()

	root, err := ParseFromString(`{"value":1}`)
	require.NoError(t, err)
	path, err := ParseJsonPath(`$`)
	require.NoError(t, err)
	iterator := NewPathIterator(root, &path)
	_, _, err = iterator.NextContext(nil)
	require.NoError(t, err)
	iterator.Close()

	// Index zero autowraps both objects and scalars; a non-zero index must not
	// manufacture a match for either representation.
	require.Equal(t, []string{`{"value":1}`}, collectIteratorMatches(t, `{"value":1}`, `$[0]`))
	require.Empty(t, collectIteratorMatches(t, `{"value":1}`, `$[1]`))
	require.Empty(t, collectIteratorMatches(t, `1`, `$[1]`))
	require.Equal(t, []string{"1"}, collectIteratorMatches(t, `1`, `$[0 to 0]`))
	require.Empty(t, collectIteratorMatches(t, `[1,2]`, `$[last-8 to last-7]`))
	require.Empty(t, collectIteratorMatches(t, `1`, `$**.value`))
	require.Empty(t, collectIteratorMatches(t, `{}`, `$.*`))
	require.Empty(t, collectIteratorMatches(t, `1`, `$.value`))
}
