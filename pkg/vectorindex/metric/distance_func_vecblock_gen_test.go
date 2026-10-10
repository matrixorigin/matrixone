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

package metric

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The generator of distance_func_vecblock_kernels.go: scalar, fully unrolled kernels over
// 16-element units for each vecf8/vecf4/vecf32 operand pair and metric.

var updateVecBlockKernels = flag.Bool("update", false, "rewrite distance_func_vecblock_kernels.go")

const vecBlockKernelsFile = "distance_func_vecblock_kernels.go"

// TestVecBlockKernelsGenerated fails when the kernels file differs from the generator output;
// -update rewrites it.
func TestVecBlockKernelsGenerated(t *testing.T) {
	src, err := genVecBlockKernels()
	require.NoError(t, err)
	if *updateVecBlockKernels {
		require.NoError(t, os.WriteFile(vecBlockKernelsFile, src, 0644))
		return
	}
	got, err := os.ReadFile(vecBlockKernelsFile)
	require.NoError(t, err)
	require.Equal(t, string(src), string(got), "regenerate with: go test -run TestVecBlockKernelsGenerated -args -update")
}

const vbGenUnit = 16

type vbGenOperand struct {
	name string // F8, F4, F32
	typ  string
}

var (
	vbGenF8  = vbGenOperand{"F8", "*types.BlockScaledCell"}
	vbGenF4  = vbGenOperand{"F4", "*types.BlockScaledCell"}
	vbGenF32 = vbGenOperand{"F32", "[]float32"}
)

var vbGenPairs = [][2]vbGenOperand{
	{vbGenF8, vbGenF8},
	{vbGenF4, vbGenF4},
	{vbGenF8, vbGenF32},
	{vbGenF4, vbGenF32},
	{vbGenF8, vbGenF4},
}

type vbGenMetric struct {
	name    string
	doc     string
	results string
	accs    []string // float32 per-vbGenUnit accumulators
	elem    func(a, b string, k int) string
	fold    string // folds the vbGenUnit accumulators into the float64 results
}

func vbGenAbs32(x string) string {
	return fmt.Sprintf("math.Float32frombits(math.Float32bits(%s) &^ (1 << 31))", x)
}

var vbGenMetrics = []vbGenMetric{
	{
		name: "Dot", doc: "the dot product", results: "r float64",
		accs: []string{"t0", "t1", "t2", "t3"},
		elem: func(a, b string, k int) string { return fmt.Sprintf("t%d += %s * %s\n", k%4, a, b) },
		fold: "r += float64(t0+t1) + float64(t2+t3)\n",
	},
	{
		name: "L2Sq", doc: "the squared L2 distance", results: "r float64",
		accs: []string{"t0", "t1", "t2", "t3"},
		elem: func(a, b string, k int) string {
			return fmt.Sprintf("d%d := %s - %s\nt%d += d%d * d%d\n", k, a, b, k%4, k, k)
		},
		fold: "r += float64(t0+t1) + float64(t2+t3)\n",
	},
	{
		name: "L1", doc: "the L1 distance", results: "r float64",
		accs: []string{"t0", "t1", "t2", "t3"},
		elem: func(a, b string, k int) string {
			return fmt.Sprintf("t%d += %s\n", k%4, vbGenAbs32(a+" - "+b))
		},
		fold: "r += float64(t0+t1) + float64(t2+t3)\n",
	},
	{
		name: "Cos", doc: "the dot product and both squared norms", results: "dot, nx, ny float64",
		accs: []string{"td0", "td1", "tx0", "tx1", "ty0", "ty1"},
		elem: func(a, b string, k int) string {
			j := k % 2
			return fmt.Sprintf("td%d += %s * %s\ntx%d += %s * %s\nty%d += %s * %s\n", j, a, b, j, a, a, j, b, b)
		},
		fold: "dot += float64(td0 + td1)\nnx += float64(tx0 + tx1)\nny += float64(ty0 + ty1)\n",
	},
}

// vbGenLoad emits the per-vbGenUnit setup for vbGenOperand v ("x" or "y").
func vbGenLoad(w *bytes.Buffer, o vbGenOperand, v string) {
	switch o.name {
	case "F8":
		fmt.Fprintf(w, "%ss := %s.Global * e8[%s.Scales[off>>5]]\n", v, v, v)
		fmt.Fprintf(w, "%se := (*[%d]byte)(%s.Elems[off : off+%d])\n", v, vbGenUnit, v, vbGenUnit)
	case "F4":
		fmt.Fprintf(w, "%sbs := f8[%s.Scales[off>>4]]\n", v, v)
		fmt.Fprintf(w, "%ss := %s.Global * %sbs\n", v, v, v)
		fmt.Fprintf(w, "%se := (*[%d]byte)(%s.Elems[off>>1 : off>>1+%d])\n", v, vbGenUnit/2, v, vbGenUnit/2)
	case "F32":
		fmt.Fprintf(w, "%sv := (*[%d]float32)(%s[off : off+%d])\n", v, vbGenUnit, v, vbGenUnit)
	}
}

// vbGenElems emits the decode of elements [g, g+4) of vbGenOperand v into v0..v3 and returns their names.
// Each decoded element is an explicit float32 conversion, which rounds the product as the
// stored value is rounded and keeps the compiler from fusing it into a later add or subtract.
func vbGenElems(w *bytes.Buffer, o vbGenOperand, v string, g int) []string {
	names := make([]string, 4)
	for k := 0; k < 4; k++ {
		names[k] = fmt.Sprintf("%s%d", v, g+k)
	}
	switch o.name {
	case "F8":
		for k := 0; k < 4; k++ {
			fmt.Fprintf(w, "%s := float32(f8[%se[%d]] * %ss)\n", names[k], v, g+k, v)
		}
	case "F4":
		fmt.Fprintf(w, "%sp%d, %sp%d := &f4[%se[%d]], &f4[%se[%d]]\n", v, g, v, g+2, v, g/2, v, g/2+1)
		fmt.Fprintf(w, "%s := float32(%sp%d[0] * %ss)\n", names[0], v, g, v)
		fmt.Fprintf(w, "%s := float32(%sp%d[1] * %ss)\n", names[1], v, g, v)
		fmt.Fprintf(w, "%s := float32(%sp%d[0] * %ss)\n", names[2], v, g+2, v)
		fmt.Fprintf(w, "%s := float32(%sp%d[1] * %ss)\n", names[3], v, g+2, v)
	case "F32":
		for k := 0; k < 4; k++ {
			fmt.Fprintf(w, "%s := %sv[%d]\n", names[k], v, g+k)
		}
	}
	return names
}

// vbGenSubnormalScale returns the condition under which F4 operand v's unit can underflow a
// decoded element to zero in float32: the same test types.BlockScaledCell.DequantizeRange applies.
// An F8 global*blockScale is never subnormal, so F8 operands have no such condition.
func vbGenSubnormalScale(o vbGenOperand, v string) string {
	if o.name != "F4" {
		return ""
	}
	return fmt.Sprintf("(%ss < 0x1p-126 && %s.Global != 0 && %sbs != 0)", v, v, v)
}

// vbGenSlowElems emits elements [g, g+4) of operand v read from its decoded unit array and returns
// their names.
func vbGenSlowElems(w *bytes.Buffer, o vbGenOperand, v string, g int) []string {
	names := make([]string, 4)
	for k := 0; k < 4; k++ {
		names[k] = fmt.Sprintf("%s%d", v, g+k)
		if o.name == "F32" {
			fmt.Fprintf(w, "%s := %sv[%d]\n", names[k], v, g+k)
		} else {
			fmt.Fprintf(w, "%s := %sd[%d]\n", names[k], v, g+k)
		}
	}
	return names
}

func vbGenUses(p [2]vbGenOperand, n string) bool { return p[0].name == n || p[1].name == n }

func genVecBlockKernels() ([]byte, error) {
	var w bytes.Buffer
	w.WriteString(`// Code generated by TestVecBlockKernelsGenerated; DO NOT EDIT.

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

package metric

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var _ = math.Float32bits
`)
	for _, m := range vbGenMetrics {
		for _, p := range vbGenPairs {
			fname := fmt.Sprintf("vecBlock%s%s%s", m.name, p[0].name, p[1].name)
			fmt.Fprintf(&w, "\n// %s returns %s of the first units*%d elements of x and y.\n", fname, m.doc, vbGenUnit)
			fmt.Fprintf(&w, "func %s(x %s, y %s, units int) (%s) {\n", fname, p[0].typ, p[1].typ, m.results)
			needF8 := vbGenUses(p, "F8") || vbGenUses(p, "F4")
			if needF8 {
				fmt.Fprintf(&w, "f8, e8, f4 := types.BlockScaledTables()\n")
				fmt.Fprintf(&w, "_, _, _ = f8, e8, f4\n")
			}
			fmt.Fprintf(&w, "for u := 0; u < units; u++ {\noff := u * %d\n", vbGenUnit)
			vbGenLoad(&w, p[0], "x")
			vbGenLoad(&w, p[1], "y")
			var conds []string
			for i, v := range []string{"x", "y"} {
				if c := vbGenSubnormalScale(p[i], v); c != "" {
					conds = append(conds, c)
				}
			}
			if len(conds) > 0 {
				// A unit whose F4 scale is subnormal decodes through At, the authoritative decode, and
				// runs the same arithmetic on the decoded values.
				fmt.Fprintf(&w, "if %s {\n", strings.Join(conds, " || "))
				for i, v := range []string{"x", "y"} {
					if p[i].name != "F32" {
						fmt.Fprintf(&w, "var %sd [%d]float32\nfor i := range %sd {\n%sd[i] = %s.At(off + i)\n}\n", v, vbGenUnit, v, v, v)
					}
				}
				fmt.Fprintf(&w, "var %s float32\n", vbGenJoin(m.accs))
				for g := 0; g < vbGenUnit; g += 4 {
					xa := vbGenSlowElems(&w, p[0], "x", g)
					yb := vbGenSlowElems(&w, p[1], "y", g)
					for k := 0; k < 4; k++ {
						w.WriteString(m.elem(xa[k], yb[k], g+k))
					}
				}
				w.WriteString(m.fold)
				w.WriteString("continue\n}\n")
			}
			fmt.Fprintf(&w, "var %s float32\n", vbGenJoin(m.accs))
			for g := 0; g < vbGenUnit; g += 4 {
				xa := vbGenElems(&w, p[0], "x", g)
				yb := vbGenElems(&w, p[1], "y", g)
				for k := 0; k < 4; k++ {
					w.WriteString(m.elem(xa[k], yb[k], g+k))
				}
			}
			w.WriteString(m.fold)
			w.WriteString("}\nreturn\n}\n")
		}
	}
	return format.Source(w.Bytes())
}

func vbGenJoin(s []string) string {
	var b bytes.Buffer
	for i, x := range s {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(x)
	}
	return b.String()
}
