// Copyright 2023 Matrix Origin
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

package moarray

//func TestNormalizeMoArray(t *testing.T) {
//	type args struct {
//		vector []float64
//	}
//	tests := []struct {
//		name string
//		args args
//		want []float64
//	}{
//		{
//			name: "Test1",
//			args: args{
//				vector: []float64{1, 2, 3},
//			},
//			want: []float64{0.2672612419124244, 0.5345224838248488, 0.8017837257372732},
//		},
//		{
//			name: "Test2",
//			args: args{
//				vector: []float64{-1, 2, 3},
//			},
//			want: []float64{-0.2672612419124244, 0.5345224838248488, 0.8017837257372732},
//		},
//		{
//			name: "Test3",
//			args: args{
//				vector: []float64{0, 0, 0},
//			},
//			want: []float64{0, 0, 0},
//		},
//		{
//			name: "Test4",
//			args: args{
//				vector: []float64{10, 3.333333333333333, 4, 5},
//			},
//			want: []float64{0.8108108108108107, 0.27027027027027023, 0.3243243243243243, 0.4054054054054054},
//		},
//		{
//			name: "Test5",
//			args: args{
//				vector: []float64{1, 2, 3.6666666666666665, 4.666666666666666},
//			},
//			want: []float64{0.15767649936829103, 0.31535299873658207, 0.5781471643504005, 0.7358236637186913},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := NormalizeMoVecf64(tt.args.vector); !assertx.InEpsilonF64Slice(tt.want, got) {
//				t.Errorf("NormalizeMoVecf64() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
