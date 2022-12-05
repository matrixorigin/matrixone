// Copyright 2022 Matrix Origin
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

package multi

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFormatWithTwo(t *testing.T) {
	testCases := []arg{
		{
			info: "Test01", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.123456", 1),
				testutil.MakeScalarVarchar("4", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332.1235", 1),
		},

		{
			info: "Test02", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.1", 1),
				testutil.MakeScalarVarchar("4", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332.1000", 1),
		},

		{
			info: "Test03", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.2", 1),
				testutil.MakeScalarVarchar("0", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332", 1),
		},

		{
			info: "Test04", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("-.12334.2", 1),
				testutil.MakeScalarVarchar("2", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("-0.12", 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			gotV, err := Format(tc.vs, tc.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, tc.expect.Col, gotV.Col)
		})
	}
}

func TestFormatWithThree(t *testing.T) {
	testCases := []arg{
		{
			info: "Test01", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.123456", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("en_US", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332.1235", 1),
		},

		{
			info: "Test02", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.1", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("en_US", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332.1000", 1),
		},

		{
			info: "Test03", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.2", 1),
				testutil.MakeScalarVarchar("0", 1),
				testutil.MakeScalarVarchar("en_US", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332", 1),
		},

		{
			info: "Test04", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("-.12334.2", 1),
				testutil.MakeScalarVarchar("2", 1),
				testutil.MakeScalarVarchar("en_US", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("-0.12", 1),
		},

		{
			info: "Test05", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.123456", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("ar_SA", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12332.1235", 1),
		},

		{
			info: "Test06", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.1", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("ar_SA", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12332.1000", 1),
		},

		{
			info: "Test07", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.2", 1),
				testutil.MakeScalarVarchar("0", 1),
				testutil.MakeScalarVarchar("ar_SA", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12332", 1),
		},

		{
			info: "Test08", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("-.12334.2", 1),
				testutil.MakeScalarVarchar("2", 1),
				testutil.MakeScalarVarchar("ar_SA", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("-0.12", 1),
		},

		{
			info: "Test09", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.123456", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("be_BY", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12.332,1235", 1),
		},

		{
			info: "Test10", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.1", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("be_BY", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12.332,1000", 1),
		},

		{
			info: "Test11", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.2", 1),
				testutil.MakeScalarVarchar("0", 1),
				testutil.MakeScalarVarchar("be_BY", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12.332", 1),
		},

		{
			info: "Test12", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("-.12334.2", 1),
				testutil.MakeScalarVarchar("2", 1),
				testutil.MakeScalarVarchar("be_BY", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("-0,12", 1),
		},

		{
			info: "Test13", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.123456", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("bg_BG", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12 332,1235", 1),
		},

		{
			info: "Test14", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.1", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("bg_BG", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12 332,1000", 1),
		},

		{
			info: "Test15", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.2", 1),
				testutil.MakeScalarVarchar("0", 1),
				testutil.MakeScalarVarchar("bg_BG", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12 332", 1),
		},

		{
			info: "Test16", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("-.12334.2", 1),
				testutil.MakeScalarVarchar("2", 1),
				testutil.MakeScalarVarchar("bg_BG", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("-0,12", 1),
		},

		{
			info: "Test17", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.123456", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("de_CH", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12'332.1235", 1),
		},

		{
			info: "Test18", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.1", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("de_CH", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12'332.1000", 1),
		},

		{
			info: "Test19", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.2", 1),
				testutil.MakeScalarVarchar("0", 1),
				testutil.MakeScalarVarchar("de_CH", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12'332", 1),
		},

		{
			info: "Test20", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("-.12334.2", 1),
				testutil.MakeScalarVarchar("2", 1),
				testutil.MakeScalarVarchar("de_CH", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("-0.12", 1),
		},
		{
			info: "Test21", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.123456", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("xxxx", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332.1235", 1),
		},

		{
			info: "Test22", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("12332.1", 1),
				testutil.MakeScalarVarchar("4", 1),
				testutil.MakeScalarVarchar("xxx", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("12,332.1000", 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			gotV, err := Format(tc.vs, tc.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, tc.expect.Col, gotV.Col)
		})
	}
}
