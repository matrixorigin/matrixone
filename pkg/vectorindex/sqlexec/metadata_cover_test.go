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

package sqlexec

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func writtenMetadata(t *testing.T) *Metadata {
	t.Helper()
	w := NewMetadataWriter()
	w.AddInt("i64", 7)
	w.AddInt8("i8", 3)
	w.AddString("s", "v")
	w.AddFloat("f", 1.5)

	blob, err := w.Marshal()
	require.NoError(t, err)
	// Marshal emits JSON text; NewMetadata takes the ByteJson binary form.
	md, err := NewMetadataFromJson(string(blob))
	require.NoError(t, err)
	return md
}

// Every writer arm round-trips through the resolver at its declared type.
func TestMetadataWriterRoundTrip(t *testing.T) {
	md := writtenMetadata(t)

	for _, c := range []struct {
		key  string
		want any
	}{
		{"i64", int64(7)},
		{"i8", int8(3)},
		{"s", "v"},
		{"f", 1.5},
	} {
		got, err := md.ResolveVariableFunc(c.key, true, false)
		require.NoError(t, err)
		require.Equal(t, c.want, got, "key %s", c.key)
	}
}

// GetString renders the captured blob.
func TestMetadataGetString(t *testing.T) {
	s := writtenMetadata(t).GetString()
	require.Contains(t, s, "i64")
	require.Contains(t, s, "s")
}

// ResolveVariableSoft answers for captured vars and returns nil for any other.
func TestMetadataResolveVariableSoft(t *testing.T) {
	md := writtenMetadata(t)

	got, err := md.ResolveVariableSoft("i64", true, false)
	require.NoError(t, err)
	require.Equal(t, int64(7), got)

	got, err = md.ResolveVariableSoft("never_captured", true, false)
	require.NoError(t, err)
	require.Nil(t, got)
}

// --- SqlProcess / SqlContext accessors -------------------------------------

// With no process, the accessors fall through to the background SqlContext.
func TestSqlProcessAccessors_SqlContext(t *testing.T) {
	ctx := context.Background()
	sc := &SqlContext{Ctx: ctx}
	sp := &SqlProcess{SqlCtx: sc}

	require.Equal(t, ctx, sp.GetContext())
	require.Equal(t, ctx, sp.GetTopContext(), "background has no separate top context")
	require.Nil(t, sp.GetResolveVariableFunc())

	fn := func(string, bool, bool) (interface{}, error) { return int64(1), nil }
	sc.SetResolveVariableFunc(fn)
	require.NotNil(t, sc.GetResolveVariableFunc())

	got, err := sp.GetResolveVariableFunc()("x", true, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
}

// A SqlProcess with neither a process nor a SqlContext reports no resolver.
func TestSqlProcessResolveVariableFunc_Empty(t *testing.T) {
	require.Nil(t, (&SqlProcess{}).GetResolveVariableFunc())
}
