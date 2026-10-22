// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"

	"github.com/stretchr/testify/require"
)

func mustSearchPath(t *testing.T, text string) *Path {
	t.Helper()
	path, err := ParseJsonPath(text)
	require.NoError(t, err)
	return &path
}

func exactSearchMatcher(want string) func([]byte) (bool, error) {
	return func(value []byte) (bool, error) {
		return bytes.Equal(value, []byte(want)), nil
	}
}

func TestSearchStringPathsReportsCompleteLocations(t *testing.T) {
	document, err := ParseFromByteSlice([]byte(
		`{"a":["x","x"],"one potato":{"x":"x"},"n":1,"bool":true,"null":null,"nested":[{"x":"x"}]}`,
	))
	require.NoError(t, err)

	paths := []*Path{
		mustSearchPath(t, "$.a[*]"),
		mustSearchPath(t, `$."one potato".x`),
		mustSearchPath(t, `$**.x`),
	}
	got, err := document.SearchStringPaths(
		context.Background(), paths, exactSearchMatcher("x"), false, 100, 4096)
	require.NoError(t, err)
	require.Equal(t, []string{
		"$.a[0]",
		"$.a[1]",
		`$."one potato".x`,
		"$.nested[0].x",
	}, got)

	got, err = document.SearchStringPaths(
		context.Background(), paths, exactSearchMatcher("x"), true, 100, 4096)
	require.NoError(t, err)
	require.Equal(t, []string{"$.a[0]"}, got)
}

func TestSearchStringPathsIgnoresNonStringLeavesAndKeys(t *testing.T) {
	document, err := ParseFromByteSlice([]byte(
		`{"x":"no-key-match","s":"1","n":1,"b":true,"z":null,"a":["1",1]}`,
	))
	require.NoError(t, err)

	got, err := document.SearchStringPaths(
		context.Background(),
		[]*Path{NewRecursiveSearchPath()},
		exactSearchMatcher("1"),
		false,
		100,
		4096,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"$.s", "$.a[0]"}, got)
}

func TestSearchStringPathsBoundsAndCancellation(t *testing.T) {
	document, err := ParseFromByteSlice([]byte(`["x","x","x"]`))
	require.NoError(t, err)
	path := mustSearchPath(t, "$[*]")

	_, err = document.SearchStringPaths(
		context.Background(), []*Path{path}, exactSearchMatcher("x"), false, 2, 4096)
	require.ErrorContains(t, err, "path-count limit")

	_, err = document.SearchStringPaths(
		context.Background(), []*Path{path}, exactSearchMatcher("x"), false, 100, len("$[0]")-1)
	require.ErrorContains(t, err, "byte limit")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = document.SearchStringPaths(
		ctx, []*Path{path}, exactSearchMatcher("x"), false, 100, 4096)
	require.ErrorIs(t, err, context.Canceled)
}
