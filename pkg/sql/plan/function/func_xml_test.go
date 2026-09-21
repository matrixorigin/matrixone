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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestXMLExtractionOracle(t *testing.T) {
	for _, tc := range []struct{ xml, path, want string }{
		{`<a><b>1</b></a>`, `/a/b`, `1`},
		{`<a><b>1</b><b>2</b></a>`, `/a/b`, `1 2`},
		{`<a id="7"/>`, `/a/@id`, `7`},
		{`<a><b/><b/></a>`, `count(/a/b)`, `2`},
		{`<a>x<b>y</b>z</a>`, `/a`, `x z`},
		{`<a>x<b>y</b>z</a>`, `/a/text()`, `x z`},
		{`<a>x<b>y</b>z</a>`, `/a|/a/b`, `x y z`},
		{`<a/><b>x</b>`, `/b`, `x`},
		{``, `/a`, ``},
		{`plain`, `/`, `plain`},
		{`<a>&amp;&#65;&unknown;</a>`, `/a`, `&amp;&#65;&unknown;`},
		{`<a><![CDATA[x<y]]></a>`, `/a`, `x<y`},
		{`<a>x<![CDATA[y]]><!--c-->z</a>`, `/a`, `x y z`},
		{`<a>  x  </a>`, `/a`, `  x  `},
		{`<a><b/><b>x</b><b/></a>`, `/a/b`, `x`},
		{`<p:a>x</p:a>`, `/p:a`, `x`},
		{`<p:a xmlns:p="u">x</p:a>`, `/p:a`, `x`},
		{`<a p:id="7"/>`, `/a/@p:id`, `7`},
		{`<a><b>1</b><b>2</b></a><a><b>3</b><b>4</b></a>`, `/a/b[1]`, `1 3`},
		{`<a><b>1</b><b>2</b></a><a><b>3</b></a>`, `//b[last()]`, `2 3`},
		{`<a><b k="X">yes</b><b>no</b></a>`, `/a/b[@k="x"]`, `yes`},
		{`<a><b k="x">1</b><b k="y">2</b></a>`, `a/b[@k][position()=2]`, `2`},
		{`<a><b k="x">1</b><b k="y">2</b></a>`, `a/b[@k][1][@k='x']`, `1`},
		{`<a><b k="x">1</b></a>`, `a/*/../b/.`, `1`},
		{`<a><b k="x">1</b></a>`, `/a/b|//b`, `1`},
		{`<a><b>YES</b></a>`, `/a[b='yes']/b`, `YES`},
		{`<a/>`, `count(/a/c)`, `0`},
		{`<a/>`, `/a/c`, ``},
	} {
		t.Run(tc.path+tc.xml, func(t *testing.T) {
			p, err := compileXMLXPath(context.Background(), tc.path)
			require.NoError(t, err)
			d, err := parseXMLFragment(context.Background(), tc.xml)
			require.NoError(t, err)
			ids, err := d.evaluate(p)
			require.NoError(t, err)
			got, err := d.extract(p, ids)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestXMLRegistration(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int32
		n    int
	}{{"extractvalue", EXTRACTVALUE, 2}, {"updatexml", UPDATEXML, 3}} {
		for _, typ := range []types.T{types.T_varchar, types.T_char, types.T_text, types.T_binary, types.T_varbinary, types.T_blob, types.T_any} {
			args := make([]types.Type, tc.n)
			exprs := make([]*plan.Expr, tc.n)
			for i := range args {
				args[i] = typ.ToType()
				exprs[i] = &plan.Expr{Typ: plan.Type{Id: int32(typ), NotNullable: true}}
			}
			r, err := GetFunctionByName(context.Background(), tc.name, args)
			require.NoError(t, err)
			require.Equal(t, tc.id, r.fid)
			require.Equal(t, types.T_text, r.retType.Oid)
			require.Equal(t, int32(types.MaxLongTextLen), r.retType.Width)
			id := encodeOverloadID(tc.id, 0)
			ov, ok := GetFunctionByIdWithoutError(id)
			require.True(t, ok)
			require.True(t, ov.CannotFold())
			require.False(t, DeduceNotNullable(id, exprs))
		}
		_, err := GetFunctionByName(context.Background(), tc.name, nil)
		require.Error(t, err)
		_, err = GetFunctionByName(context.Background(), tc.name, []types.Type{types.T_int64.ToType(), types.T_varchar.ToType()})
		require.Error(t, err)
	}
}

func TestXMLUpdateOracle(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct{ xml, path, replacement, want string }{
		{`<a><b>1</b></a>`, `/a/b`, `<c>2</c>`, `<a><c>2</c></a>`},
		{`<a><b>1</b><b>2</b></a>`, `/a/b`, `<c/>`, `<a><b>1</b><b>2</b></a>`},
		{`<a><b>1</b></a>`, `/a/c`, `<c/>`, `<a><b>1</b></a>`},
		{`<a k="7"><b/></a>`, `/a/@k`, `z`, `<a z><b/></a>`},
		{`<a />`, `/a`, `not xml`, `not xml`},
		{`<a/>`, `/`, `raw`, `raw`},
		{`<?p x?><a k='&amp;'><!--c--><b /></a>`, `//b`, `<z>new</z>`, `<?p x?><a k='&amp;'><!--c--><z>new</z></a>`},
	} {
		t.Run(tc.path+tc.xml, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.xml}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.path}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.replacement}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil), UpdateXML)
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestXMLMalformedAndUnsupported(t *testing.T) {
	for _, s := range []string{`<a>`, `<a></b>`, `<a/><b`, `<a x='1' x='2'/>`, `<a x=1/>`, `<a x='<'>`, `<!DOCTYPE a><a/>`, `<a>\x00</a>`} {
		if s == `<a>\x00</a>` {
			s = "<a>\x00</a>"
		}
		_, err := parseXMLFragment(context.Background(), s)
		require.ErrorIs(t, err, errXMLMalformed, s)
	}
	for _, s := range []string{"", "[", "/a[", "//", "/a/", "/a[0]", "/a[-1]", "/a/ancestor::b", "sum(/a)", "1+2", "true()", "$x", "count(/a/text())", "/a[text()='x']", "/a/@"} {
		_, err := compileXMLXPath(context.Background(), s)
		require.Error(t, err, s)
	}
}

func TestXMLLimitsAndCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := parseXMLFragment(ctx, "<a/>")
	require.ErrorIs(t, err, context.Canceled)
	_, err = compileXMLXPath(ctx, "/a")
	require.ErrorIs(t, err, context.Canceled)
	_, err = parseXMLFragment(context.Background(), strings.Repeat("x", xmlInputLimit+1))
	require.ErrorContains(t, err, "8 MiB")
	_, err = compileXMLXPath(context.Background(), strings.Repeat("a", xmlXPathLimit+1))
	require.ErrorContains(t, err, "16 KiB")
	_, err = parseXMLFragment(context.Background(), strings.Repeat("<a>", xmlDepthLimit)+strings.Repeat("</a>", xmlDepthLimit))
	require.NoError(t, err)
	_, err = parseXMLFragment(context.Background(), strings.Repeat("<a>", xmlDepthLimit+1)+strings.Repeat("</a>", xmlDepthLimit+1))
	require.ErrorContains(t, err, "nesting")
	_, err = compileXMLXPath(context.Background(), strings.Repeat("/a", 129))
	require.ErrorContains(t, err, "step limit")
	d, err := parseXMLFragment(context.Background(), "<a>x</a>")
	require.NoError(t, err)
	p, err := compileXMLXPath(context.Background(), "/a")
	require.NoError(t, err)
	d.budget.work = xmlWorkLimit
	_, err = d.evaluate(p)
	require.ErrorContains(t, err, "resource limit")
	d.budget.work = 0
	d.budget.scratch = xmlScratchLimit
	_, err = d.evaluate(p)
	require.ErrorContains(t, err, "resource limit")
}

func TestXMLRepeatedPredicatesConsumeWork(t *testing.T) {
	// Both inputs fit all admission limits, but positional predicates must
	// consume work too: this would otherwise execute 30 million iterations.
	p, err := compileXMLXPath(context.Background(), "//b"+strings.Repeat("[1]", 1000))
	require.NoError(t, err)
	d, err := parseXMLFragment(context.Background(), strings.Repeat("<a><b/></a>", 30000))
	require.NoError(t, err)
	_, err = d.evaluate(p)
	require.ErrorContains(t, err, "resource limit")
	require.Equal(t, xmlWorkLimit, d.budget.work)
}

func TestXMLOversizedXPathAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	doc := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	defer doc.Free(proc.Mp())
	path, err := vector.NewConstBytes(types.T_text.ToType(), []byte(strings.Repeat("a", xmlXPathLimit+1)), 1, proc.Mp())
	require.NoError(t, err)
	defer path.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.ErrorContains(t, ExtractValue([]*vector.Vector{doc, path}, result, proc, 1, nil), "16 KiB")
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, ExtractValue([]*vector.Vector{doc, path}, result, proc, 1, &FunctionSelectList{AllNull: true}))
}

func TestXMLVectorWarningsMasksAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	sink := &uncompressWarningSink{}
	proc.WarningSink = sink
	docs := testutil.NewVectorWithNulls(5, types.T_varchar.ToType(), proc.Mp(), false, []bool{false, false, false, false, true}, []string{"<a>x</a>", "<a>", "<a/>", "<a>y</a>", ""})
	defer docs.Free(proc.Mp())
	path, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("/a"), 5, proc.Mp())
	require.NoError(t, err)
	defer path.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(5))
	require.NoError(t, ExtractValue([]*vector.Vector{docs, path}, result, proc, 5, nil))
	v := result.GetResultVector()
	require.Equal(t, "x", v.GetStringAt(0))
	require.True(t, v.IsNull(1))
	require.Equal(t, "", v.GetStringAt(2))
	require.Equal(t, "y", v.GetStringAt(3))
	require.True(t, v.IsNull(4))
	require.Equal(t, uint64(1), sink.total)
	require.Equal(t, uint16(1525), sink.records[0].code)
	require.NoError(t, result.PreExtendAndReset(5))
	require.NoError(t, ExtractValue([]*vector.Vector{docs, path}, result, proc, 5, &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true, true, true}}))
	require.Equal(t, uint64(1), sink.total, "masked malformed input must not warn")
	other, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("count(/a)"), 1, proc.Mp())
	require.NoError(t, err)
	defer other.Free(proc.Mp())
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, ExtractValue([]*vector.Vector{docs, other}, result, proc, 1, nil))
	require.Equal(t, "1", result.GetResultVector().GetStringAt(0))
	require.NoError(t, result.PreExtendAndReset(0))
	require.NoError(t, ExtractValue([]*vector.Vector{docs, path}, result, proc, 0, nil))
}

func TestXMLXPathPrecedesNullDocument(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	doc := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	defer doc.Free(proc.Mp())
	path, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("["), 1, proc.Mp())
	require.NoError(t, err)
	defer path.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	require.ErrorContains(t, ExtractValue([]*vector.Vector{doc, path}, result, proc, 1, nil), "XPATH")
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, ExtractValue([]*vector.Vector{doc, path}, result, proc, 1, &FunctionSelectList{AllNull: true}))
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, ExtractValue([]*vector.Vector{path, doc}, result, proc, 1, nil))
	require.True(t, result.GetResultVector().IsNull(0))
}
