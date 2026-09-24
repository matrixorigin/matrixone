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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func cleanupXMLFunctionTestCase(t *testing.T, fc *FunctionTestCase) {
	t.Helper()
	t.Cleanup(func() {
		for _, input := range fc.parameters {
			input.Free(fc.proc.Mp())
		}
		fc.result.Free()
	})
}

func TestXMLExtractionOracle(t *testing.T) {
	for _, tc := range []struct{ xml, path, want string }{
		{`<a><b>1</b></a>`, `/a/b`, `1`},
		{`<a><b>1</b><b>2</b></a>`, `/a/b`, `1 2`},
		{`<a id="7"/>`, `/a/@id`, `7`},
		{`<a><b/><b/></a>`, `count(/a/b)`, `2`},
		{`<a><b>1</b><b>2</b></a>`, `sum(/a/b)`, `3`},
		{`<a><b>1</b><b>2</b></a>`, `count(/a/b)=2`, `1`},
		{`<a><b>1</b><b>2</b></a>`, `count(/a/b)!=2`, `0`},
		{`<a><b>1</b><b>2</b></a>`, `sum(/a/b)>2`, `1`},
		{`<a><b>1</b><b>2</b></a>`, `sum(/a/b)=count(/a/b)`, `0`},
		{`<a/>`, `9007199254740993=9007199254740992`, `0`},
		{`<a/>`, `9007199254740993`, `9007199254740993`},
		{`<a/>`, `0002`, `2`},
		{`<a>1e20</a>`, `sum(/a)`, `1e20`},
		{`<a>1e-20</a>`, `sum(/a)`, `1e-20`},
		{`<a>1000000000000000.1</a>`, `sum(/a)`, `1000000000000000.1`},
		{`<a>-1000000000000000.1</a>`, `sum(/a)`, `-1000000000000000.1`},
		{`<a>1e15</a>`, `sum(/a)`, `1e15`},
		{`<a>1e-15</a>`, `sum(/a)`, `0.000000000000001`},
		{`<a>1e-16</a>`, `sum(/a)`, `1e-16`},
		{"<a>\v2</a>", `sum(/a)`, `2`},
		{"<a>\f2</a>", `sum(/a)`, `2`},
		{"<a k=\"\v2\"/>", `sum(/a/@k)`, `2`},
		{"<a k=\"\f2\"/>", `sum(/a/@k)`, `2`},
		{"<a>\v2</a>", `/a`, "\v2"},
		{"<a k=\"\f2\"/>", `/a/@k`, "\f2"},
		{`<a><b>1<c>2</c>3</b></a>`, `sum(/a/b)`, `4`},
		{`<a/>`, `sum(/a/b)`, `0`},
		{`<a>x<b>y</b>z</a>`, `/a`, `x z`},
		{`<a>x<b>y</b>z</a>`, `/a/text()`, `x z`},
		{`<a>x<b>y</b>z</a>`, `//text()`, `x y z`},
		{`<a>x<b>y</b>z</a>`, `/a//text()`, `x y z`},
		{`<a>x<b>y</b>z</a>`, `/a/b/text()`, `y`},
		{`pre<a>x<b>y</b>z</a>post`, `//text()`, `pre x y z post`},
		{`<a>x<a>y</a>z</a>`, `//a//text()`, `x y z`},
		{`<a>x</a><a>x</a>`, `//text()`, `x x`},
		{`<a k="7">x<b>y</b></a>`, `/a/@k//text()`, ``},
		{`<a k="7">x<b>y</b></a>`, `/a/@k|/a//text()`, `7 x y`},
		{`<_p:_a>x</_p:_a>`, `/_p:_a`, `x`},
		{`<é:β>x</é:β>`, `/é:β`, `x`},
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
		{`<a><b>1</b><b>2</b></a>`, `/a/b[position()=last()]`, `2`},
		{`<a><b>1</b><b>2</b><b>3</b></a>`, `/a/b[position()<last()]`, `1 2`},
		{`<a><b>1</b><b>2</b></a><a><b>3</b><b>4</b></a>`, `/a/b[position()=last()]`, `2 4`},
		{`<a><b k="X">yes</b><b>no</b></a>`, `/a/b[@k="x"]`, `yes`},
		{`<a><b k="x">1</b><b k="y">2</b></a>`, `a/b[@k][position()=2]`, `2`},
		{`<a><b k="x">1</b><b k="y">2</b></a>`, `a/b[@k][1][@k='x']`, `1`},
		{`<a><b k="x">1</b></a>`, `a/*/../b/.`, `1`},
		{`<a><b k="x">1</b></a>`, `/a/b|//b`, `1`},
		{`<a><b>YES</b></a>`, `/a[b='yes']/b`, `YES`},
		{`<p:a p:id="7"><p:b>x</p:b></p:a>`, `/p:a[@p:id="7"][p:b="x"]/@p:id`, `7`},
		{`<a><b>x<c/>y</b></a>`, `/a[b="x"]/b`, `x y`},
		{`<a><b>x<c/>y</b></a>`, `/a[b="y"]/b`, `x y`},
		{`<a><b>x<c/>y</b></a>`, `count(/a[b="xy"])`, `0`},
		{`<a><b>x<c/>y</b></a>`, `count(/a[b="x y"])`, `0`},
		{`<a><b><c>x</c></b></a>`, `count(/a[b="x"])`, `0`},
		{`<a><b/><b></b></a>`, `count(/a[b=""])`, `0`},
		{`<a><b>x</b><b>y</b></a>`, `count(/a[b="x"][b="y"])`, `1`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a/@k//.`, `7`},
		{`<a k="7">t<b k="8">u</b></a>`, `count(/a/@k//.)`, `1`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a/@k//..`, `t`},
		{`<a k="7">t<b k="8">u</b></a>`, `count(/a//.)`, `2`},
		{`<a k="7">t<b k="8">u</b></a>`, `count(/a//@k//.)`, `2`},
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

func TestXMLScalarPublicEntrypoints(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	const twoChildren = `<a><b>1</b><b>2</b></a>`
	for _, tc := range []struct{ xml, path, want string }{
		{twoChildren, `sum(/a/b)`, `3`},
		{twoChildren, `count(/a/b)=2`, `1`},
		{twoChildren, `sum(/a/b)>=4`, `0`},
		{twoChildren, `9007199254740993=9007199254740992`, `0`},
		{twoChildren, `9007199254740993`, `9007199254740993`},
		{twoChildren, `sum(/a/b)=3`, `1`},
		{twoChildren, `/a/b[position()=last()]`, `2`},
		{`<a>1000000000000000.1</a>`, `sum(/a)`, `1000000000000000.1`},
		{"<a>\v2</a>", `sum(/a)`, `2`},
	} {
		t.Run(tc.path+"/"+tc.xml, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.xml}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.path}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil), ExtractValue)
			cleanupXMLFunctionTestCase(t, &fc)
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestXMLUpdateOracle(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct{ xml, path, replacement, want string }{
		{`<a><b>1</b></a>`, `/a/b`, `<c>2</c>`, `<a><c>2</c></a>`},
		{`<a><b>1</b><b>2</b></a>`, `/a/b`, `<c/>`, `<a><b>1</b><b>2</b></a>`},
		{`<a><b>1</b></a>`, `/a/c`, `<c/>`, `<a><b>1</b></a>`},
		{`<a><b>1</b><b>2</b></a>`, `/a/b[position()=last()]`, `<c/>`, `<a><b>1</b><c/></a>`},
		{"<a>\v2<b/></a>", `/a/b`, `<c/>`, "<a>\v2<c/></a>"},
		{`<a k="7"><b/></a>`, `/a/@k`, `z`, `<a z><b/></a>`},
		{`<a />`, `/a`, `not xml`, `not xml`},
		{`<a/>`, `/`, `raw`, `raw`},
		{`<?p x?><a k='&amp;'><!--c--><b /></a>`, `//b`, `<z>new</z>`, `<?p x?><a k='&amp;'><!--c--><z>new</z></a>`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a/@k//.`, `z`, `<a z>t<b k="8">u</b></a>`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a/@k//./.`, `z`, `<a z>t<b k="8">u</b></a>`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a/@k//..`, `z`, `z`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a/@k//*`, `z`, `<a k="7">t<b k="8">u</b></a>`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a/@k//@*`, `z`, `<a k="7">t<b k="8">u</b></a>`},
		{`<a k="7">t<b k="8">u</b></a>`, `/a//@k//.`, `z`, `<a k="7">t<b k="8">u</b></a>`},
	} {
		t.Run(tc.path+tc.xml, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.xml}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.path}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.replacement}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil), UpdateXML)
			cleanupXMLFunctionTestCase(t, &fc)
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestXMLUpdateTextTargets(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	for _, tc := range []struct {
		name, xml, path, want string
	}{
		{"root", `<a>x</a>`, "/a/text()", "q"},
		{"nested", `<a><b>x</b></a>`, "/a/b/text()", `<a>q</a>`},
		{"empty element", `<a/>`, "/a/text()", "q"},
		{"split text", `<a>x<b/>z</a>`, "/a/text()", "q"},
		{"CDATA", `<a><![CDATA[x]]></a>`, "/a/text()", "q"},
		{"two targets", `<a><b>x</b><b>y</b></a>`, "/a/b/text()", `<a><b>x</b><b>y</b></a>`},
		{"missing", `<a/>`, "/missing/text()", `<a/>`},
		{"union dedup", `<a>x</a>`, "/a/text()|/a", "q"},
		{"union two targets", `<a><b>x</b></a>`, "/a/text()|/a/b/text()", `<a><b>x</b></a>`},
		{"descendant single", `<a>x</a>`, "/a//text()", "q"},
		{"descendant multiple", `<a>x<b>y</b></a>`, "/a//text()", `<a>x<b>y</b></a>`},
		{"document and element", `<a>x</a>`, "//text()", `<a>x</a>`},
		{"document only", `plain`, "//text()", "q"},
		{"attribute context", `<a k="7"/>`, "/a/@k/text()", `<a q/>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.xml}, nil),
				NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.path}, nil),
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"q"}, nil),
			}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil), UpdateXML)
			cleanupXMLFunctionTestCase(t, &fc)
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
	doc := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	xpath, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("/a/text()"), 1, proc.Mp())
	replacement := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() { doc.Free(proc.Mp()) })
	t.Cleanup(func() { xpath.Free(proc.Mp()) })
	t.Cleanup(func() { replacement.Free(proc.Mp()) })
	result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
	t.Cleanup(result.Free)
	require.NoError(t, result.PreExtendAndReset(1))
	err = UpdateXML([]*vector.Vector{doc, xpath, replacement}, result, proc, 1, nil)
	require.NoError(t, err)
	require.True(t, result.GetResultVector().IsNull(0))
}

func TestXMLMalformedAndUnsupported(t *testing.T) {
	for _, s := range []string{"/a[\v]", "/a[\f]"} {
		_, err := compileXMLXPath(context.Background(), s)
		require.Error(t, err)
	}
	for _, s := range []string{`<a>`, `<a></b>`, `<a/><b`, `<a x='1' x='2'/>`, `<a x=1/>`, `<a x='<'>`, `<!DOCTYPE a><a/>`, `<a>\x00</a>`} {
		if s == `<a>\x00</a>` {
			s = "<a>\x00</a>"
		}
		_, err := parseXMLFragment(context.Background(), s)
		require.ErrorIs(t, err, errXMLMalformed, s)
	}
	for _, s := range []string{"", "[", "/a[", "//", "/a/", "/a[0]", "/a[-1]", "/a/ancestor::b", "sum()", "sum(/a", "sum (/a)", "sum(/a/text())", "count(/a)=/a", "count(/a)=2e0", "count(/a)=+2", "count(/a)=2.5", ".5", "2.5", "9223372036854775808>0", "-9223372036854775808", "-9223372036854775808<0", "1.234567890123456789", "/a[position()=]", "/a[position()=+1]", "/a[position()=2e0]", "/a[position()=2.5]", "/a[position()>-9223372036854775808]", "1+2", "true()", "$x", "count(/a/text())", "count(//text())", "count(/a|/a//text())", "/a[text()='x']", "/a/@", "/:a", "/a:", "/a:b:c", "/a::b", "/p:1a", "/p:-a", "/a/@:k", "/a/@k:", "/a/@p:k:q", "/a[@:k]", "/a[@p:k:q='v']", "/a[:b='v']", "/a[p:b:c='v']", "/*:a", "/p:*", "/a/@p:*", "/a[@p:*='v']", "/a/text()[1]"} {
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
	badNUL, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("<a>\x002</a>"), 1, proc.Mp())
	require.NoError(t, err)
	defer badNUL.Free(proc.Mp())
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, ExtractValue([]*vector.Vector{badNUL, path}, result, proc, 1, nil))
	require.True(t, result.GetResultVector().IsNull(0))
	require.Equal(t, uint64(2), sink.total)
	require.Equal(t, uint16(1525), sink.records[1].code)
}

func TestXMLWarningRetentionUsesProcessLimits(t *testing.T) {
	for _, tc := range []struct {
		name            string
		update          bool
		maxErrorCount   int
		queryLimit      int64
		rowCount        int
		wantRecordCount int
	}{
		{name: "extract capacity above default", maxErrorCount: 2048, rowCount: 1100, wantRecordCount: 1100},
		{name: "extract explicit zero", maxErrorCount: 0, rowCount: 3, wantRecordCount: 0},
		{
			name:            "update shared budget",
			update:          true,
			maxErrorCount:   2048,
			queryLimit:      int64(process.WarningDiagnosticRecordBytes("Incorrect XML value: malformed XML fragment")),
			rowCount:        2,
			wantRecordCount: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			proc.Base.SessionInfo.MaxErrorCount = tc.maxErrorCount
			proc.Base.SessionInfo.MaxErrorCountSet = true
			warnings := &uncompressWarningSink{}
			proc.WarningSink = warnings

			doc, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("<a>"), tc.rowCount, proc.Mp())
			require.NoError(t, err)
			defer doc.Free(proc.Mp())
			path, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("/a"), tc.rowCount, proc.Mp())
			require.NoError(t, err)
			defer path.Free(proc.Mp())
			parameters := []*vector.Vector{doc, path}
			if tc.update {
				replacement, replacementErr := vector.NewConstBytes(types.T_varchar.ToType(), []byte("<b/>"), tc.rowCount, proc.Mp())
				require.NoError(t, replacementErr)
				defer replacement.Free(proc.Mp())
				parameters = append(parameters, replacement)
			}
			result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(tc.rowCount))
			if tc.queryLimit > 0 {
				proc.Base.Lim.Size = tc.queryLimit
			}

			var callErr error
			if tc.update {
				callErr = UpdateXML(parameters, result, proc, tc.rowCount, nil)
			} else {
				callErr = ExtractValue(parameters, result, proc, tc.rowCount, nil)
			}
			require.NoError(t, callErr)
			require.Equal(t, uint64(tc.rowCount), warnings.total)
			require.Len(t, warnings.records, tc.wantRecordCount)
		})
	}
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
