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

package elkans

import (
	"github.com/matrixorigin/matrixone/pkg/common/assertx"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
	"reflect"
	"testing"
)

func Test_NewKMeans(t *testing.T) {
	type constructorArgs struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	tests := []struct {
		name    string
		fields  constructorArgs
		wantErr bool
	}{
		{
			name: "Test 1 - Dimension mismatch",
			fields: constructorArgs{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			wantErr: true,
		},
		{
			name: "Test 2 - ClusterCnt> len(vectorList)",
			fields: constructorArgs{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 6},
				},
				clusterCnt:     4,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			wantErr: true,
		},
		{
			name: "Test 3 - ClusterCnt> len(vectorList)",
			fields: constructorArgs{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 6},
				},
				clusterCnt:     4,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType, false) //<-- Not Spherical Kmeans UT
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKMeans() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_Cluster(t *testing.T) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})
	type constructorArgs struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	tests := []struct {
		name    string
		fields  constructorArgs
		want    [][]float64
		wantErr bool
		wantSSE float64
	}{
		{
			name: "Test 1 - Skewed data (Random Init)",
			fields: constructorArgs{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{10, 2, 4, 5},
					{10, 3, 4, 5},
					{10, 5, 4, 5},
					{10, 2, 4, 5},
					{10, 3, 4, 5},
					{10, 5, 4, 5},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			want: [][]float64{
				//{0.15915269938161652, 0.31830539876323305, 0.5757527355814478, 0.7349054349630643}, // approx {1, 2, 3.6666666666666665, 4.666666666666666}
				//{0.8077006350571528, 0.26637173227965466, 0.3230802540228611, 0.4038503175285764},  // approx {10, 3.333333333333333, 4, 5}
				{10, 3.333333333333333, 4, 5},
				{1, 2, 3.6666666666666665, 4.666666666666666},
			},
			//wantSSE: 0.0657884123589134,
			wantSSE: 12,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterer, _ := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType, false)
			got, err := clusterer.Cluster()
			if (err != nil) != tt.wantErr {
				t.Errorf("Cluster() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !assertx.InEpsilonF64Slices(tt.want, got) {
				t.Errorf("Cluster() got = %v, want %v", got, tt.want)
			}

			if !assertx.InEpsilonF64(tt.wantSSE, clusterer.SSE()) {
				t.Errorf("SSE() got = %v, want %v", clusterer.SSE(), tt.wantSSE)
			}
		})
	}
}

func TestElkanClusterer_initBounds(t *testing.T) {
	type constructorArgs struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	type internalState struct {
		centroids [][]float64
	}
	type wantState struct {
		vectorMetas []vectorMeta
		assignment  []int
	}
	tests := []struct {
		name   string
		fields constructorArgs
		state  internalState
		want   wantState
	}{
		{
			name: "Test 1",
			fields: constructorArgs{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{10, 20, 30, 40},
					{11, 23, 33, 47},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			state: internalState{
				centroids: [][]float64{
					{1, 2, 3, 4},
					{10, 20, 30, 40},
				},
			},
			want: wantState{
				vectorMetas: []vectorMeta{
					{
						lower:     []float64{0, 49.29503017546495},
						upper:     0,
						recompute: true,
					},
					{
						lower:     []float64{1.4142135623730951, 48.02082881417188},
						upper:     1.4142135623730951,
						recompute: true,
					},
					{
						lower:     []float64{1.4142135623730951, 48.02082881417188},
						upper:     1.4142135623730951,
						recompute: true,
					},
					{
						lower:     []float64{49.29503017546495, 0},
						upper:     0,
						recompute: true,
					},
					{
						lower:     []float64{57.35852159879994, 8.246211251235321},
						upper:     8.246211251235321,
						recompute: true,
					},
				},
				assignment: []int{0, 0, 0, 1, 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km, err := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType, false)

			// NOTE: here km.Normalize() is skipped as we not calling km.Cluster() in this test.
			// Here we are only testing the working of initBounds() function.
			if err != nil {
				t.Errorf("Error while creating KMeans object %v", err)
			}
			if ekm, ok := km.(*ElkanClusterer); ok {
				ekm.centroids, _ = moarray.ToGonumVectors[float64](tt.state.centroids...)
				ekm.initBounds()
				if !reflect.DeepEqual(ekm.assignments, tt.want.assignment) {
					t.Errorf("assignments got = %v, want %v", ekm.assignments, tt.want.assignment)
				}

				for i := 0; i < len(tt.want.vectorMetas); i++ {
					if !assertx.InEpsilonF64Slice(tt.want.vectorMetas[i].lower, ekm.vectorMetas[i].lower) {
						t.Errorf("vectorMetas got = %v, want %v", ekm.vectorMetas, tt.want.vectorMetas)
					}

					if !assertx.InEpsilonF64(tt.want.vectorMetas[i].upper, ekm.vectorMetas[i].upper) {
						t.Errorf("vectorMetas got = %v, want %v", ekm.vectorMetas, tt.want.vectorMetas)
					}

					if ekm.vectorMetas[i].recompute != tt.want.vectorMetas[i].recompute {
						t.Errorf("vectorMetas got = %v, want %v", ekm.vectorMetas, tt.want.vectorMetas)
					}
				}

			} else if !ok {
				t.Errorf("km not of type ElkanClusterer")
			}

		})
	}
}

func TestElkanClusterer_computeCentroidDistances(t *testing.T) {
	type constructorArgs struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	type internalState struct {
		centroids [][]float64
	}
	type wantState struct {
		halfInterCentroidDistMatrix [][]float64
		minHalfInterCentroidDist    []float64
	}
	tests := []struct {
		name   string
		fields constructorArgs
		state  internalState
		want   wantState
	}{
		{
			name: "Test 1",
			fields: constructorArgs{
				vectorList: [][]float64{
					// This is dummy data. Won't be used for this test function.
					{0}, {0}, {0}, {0}, {0}, {0},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			state: internalState{
				centroids: [][]float64{
					{1, 2, 3, 4},
					{10, 20, 30, 40},
				},
			},
			want: wantState{
				halfInterCentroidDistMatrix: [][]float64{
					{0, 24.647515087732476}, {24.647515087732476, 0},
				},
				minHalfInterCentroidDist: []float64{24.647515087732476, 24.647515087732476},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km, err := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType, false)

			if err != nil {
				t.Errorf("Error while creating KMeans object %v", err)
			}
			if ekm, ok := km.(*ElkanClusterer); ok {
				ekm.centroids, _ = moarray.ToGonumVectors[float64](tt.state.centroids...)
				ekm.computeCentroidDistances()

				// NOTE: here we are not considering the vectors in the vectorList. Hence we don't need to worry about
				// the normalization impact. Here we are only testing the working of computeCentroidDistances() function.

				if !assertx.InEpsilonF64Slices(tt.want.halfInterCentroidDistMatrix, ekm.halfInterCentroidDistMatrix) {
					t.Errorf("halfInterCentroidDistMatrix got = %v, want %v", ekm.halfInterCentroidDistMatrix, tt.want.halfInterCentroidDistMatrix)
				}

				if !assertx.InEpsilonF64Slice(tt.want.minHalfInterCentroidDist, ekm.minHalfInterCentroidDist) {
					t.Errorf("minHalfInterCentroidDist got = %v, want %v", ekm.minHalfInterCentroidDist, tt.want.minHalfInterCentroidDist)
				}

				if !reflect.DeepEqual(ekm.minHalfInterCentroidDist, tt.want.minHalfInterCentroidDist) {
					t.Errorf("minHalfInterCentroidDist got = %v, want %v", ekm.minHalfInterCentroidDist, tt.want.minHalfInterCentroidDist)
				}

			} else if !ok {
				t.Errorf("km not of type ElkanClusterer")
			}

		})
	}
}

func TestElkanClusterer_recalculateCentroids(t *testing.T) {
	type constructorArgs struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	type internalState struct {
		assignments []int
	}
	type wantState struct {
		centroids [][]float64
	}
	tests := []struct {
		name   string
		fields constructorArgs
		state  internalState
		want   wantState
	}{
		{
			name: "Test 1",
			fields: constructorArgs{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{10, 20, 30, 40},
					{11, 23, 33, 47},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			state: internalState{
				assignments: []int{0, 0, 0, 1, 1},
			},
			want: wantState{
				centroids: [][]float64{
					{1, 2, 3.6666666666666665, 4.666666666666666},
					{10.5, 21.5, 31.5, 43.5},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km, err := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType, false)

			if err != nil {
				t.Errorf("Error while creating KMeans object %v", err)
			}
			if ekm, ok := km.(*ElkanClusterer); ok {
				ekm.assignments = tt.state.assignments

				// NOTE: here km.Normalize() is skipped as we not calling km.Cluster() in this test.
				// Here we are only testing the working of recalculateCentroids() function.

				got := ekm.recalculateCentroids()
				arrays, _ := moarray.ToMoArrays[float64](got)
				if !assertx.InEpsilonF64Slices(tt.want.centroids, arrays) {
					t.Errorf("centroids got = %v, want %v", arrays, tt.want.centroids)
				}

			} else if !ok {
				t.Errorf("km not of type ElkanClusterer")
			}

		})
	}
}

func TestElkanClusterer_updateBounds(t *testing.T) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})
	type constructorArgs struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	type internalState struct {
		vectorMetas  []vectorMeta
		centroids    [][]float64
		newCentroids [][]float64
	}
	type wantState struct {
		vectorMetas []vectorMeta
	}
	tests := []struct {
		name   string
		fields constructorArgs
		state  internalState
		want   wantState
	}{
		{
			name: "Test 1",
			fields: constructorArgs{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{10, 20, 30, 40},
					{11, 23, 33, 47},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2Distance,
				initType:       kmeans.Random,
			},
			state: internalState{
				vectorMetas: []vectorMeta{
					{
						lower:     []float64{0, 49.29503017546495},
						upper:     0,
						recompute: false,
					},
					{
						lower:     []float64{1.4142135623730951, 48.02082881417188},
						upper:     1.4142135623730951,
						recompute: false,
					},
					{
						lower:     []float64{1.4142135623730951, 48.02082881417188},
						upper:     1.4142135623730951,
						recompute: false,
					},
					{
						lower:     []float64{49.29503017546495, 0},
						upper:     0,
						recompute: false,
					},
					{
						lower:     []float64{57.358521598799946, 8.246211251235321},
						upper:     8.246211251235321,
						recompute: false,
					},
				},
				centroids: [][]float64{
					{1, 2, 3, 4},
					{10, 20, 30, 40},
				},
				newCentroids: [][]float64{
					{1, 2, 3.6666666666666665, 4.666666666666667},
					{10.5, 21.5, 31.5, 43.5},
				},
			},
			want: wantState{
				vectorMetas: []vectorMeta{
					{
						lower:     []float64{0, 45.17192454984729},
						upper:     0.9428090415820634,
						recompute: true,
					},
					{
						lower:     []float64{0.4714045207910318, 43.89772318855422},
						upper:     2.3570226039551585,
						recompute: true,
					},
					{
						lower:     []float64{0.4714045207910318, 43.89772318855422},
						upper:     2.3570226039551585,
						recompute: true,
					},
					{
						lower:     []float64{48.352221133882885, 0},
						upper:     0.9428090415820634,
						recompute: true,
					},
					{
						lower:     []float64{56.41571255721788, 4.123105625617661},
						upper:     9.189020292817384,
						recompute: true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km, err := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType, false)

			if err != nil {
				t.Errorf("Error while creating KMeans object %v", err)
			}
			if ekm, ok := km.(*ElkanClusterer); ok {
				ekm.vectorMetas = tt.state.vectorMetas
				ekm.centroids, _ = moarray.ToGonumVectors[float64](tt.state.centroids...)

				// NOTE: here km.Normalize() is skipped as we not calling km.Cluster() in this test.
				// Here we are only testing the working of updateBounds() function.
				gonumVectors, _ := moarray.ToGonumVectors[float64](tt.state.newCentroids...)
				ekm.updateBounds(gonumVectors)

				for i := 0; i < len(tt.want.vectorMetas); i++ {
					if !assertx.InEpsilonF64Slice(tt.want.vectorMetas[i].lower, ekm.vectorMetas[i].lower) {
						t.Errorf("vectorMetas got = %v, want %v", ekm.vectorMetas, tt.want.vectorMetas)
					}

					if !assertx.InEpsilonF64(tt.want.vectorMetas[i].upper, ekm.vectorMetas[i].upper) {
						t.Errorf("vectorMetas got = %v, want %v", ekm.vectorMetas, tt.want.vectorMetas)
					}

					if ekm.vectorMetas[i].recompute != tt.want.vectorMetas[i].recompute {
						t.Errorf("vectorMetas got = %v, want %v", ekm.vectorMetas, tt.want.vectorMetas)
					}
				}

			} else if !ok {
				t.Errorf("km not of type ElkanClusterer")
			}

		})
	}
}
