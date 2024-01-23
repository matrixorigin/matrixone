// Copyright 2024 Matrix Origin
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

package faiss

import "C"
import "github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"

/*
#cgo CPPFLAGS: -Ithirdparty/libfaiss-src/c_api
#cgo CFLAGS: -Ithirdparty/libfaiss-src/c_api
#cgo darwin LDFLAGS: -Lthirdparty/runtimes/osx-arm64 -lfaiss_c -lfaiss -lomp
#cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
#cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all

#include <stdlib.h>
#include <Clustering_c.h>
#include <impl/AuxIndexStructures_c.h>
#include <index_factory_c.h>
#include <error_c.h>
*/
import "C"
import (
	"errors"
)

type Faiss struct {
	data       [][]float32
	clusterCnt int
}

var _ kmeans.Clusterer = (*Faiss)(nil)

func NewFaiss(vectors [][]float64, clusterCnt int) (kmeans.Clusterer, error) {
	return &Faiss{
		data:       vecf64ToVecf32(vectors),
		clusterCnt: clusterCnt,
	}, nil
}

func (f *Faiss) InitCentroids() error {
	return nil
}

func (f *Faiss) Cluster() (centroids [][]float64, error error) {
	if len(f.data) == 0 {
		return nil, errors.New("empty rows")
	}
	if len(f.data[0]) == 0 {
		return nil, errors.New("zero dimensions")
	}

	rowCnt := int64(len(f.data))
	dims := int64(len(f.data[0]))
	vectorFlat := make([]float32, dims*rowCnt)
	for r := int64(0); r < rowCnt; r++ {
		for c := int64(0); c < dims; c++ {
			vectorFlat[(r*dims)+c] = f.data[r][c]
		}
	}

	centroidsFlat := make([]float32, dims*int64(f.clusterCnt))
	var qError float32
	_ = C.faiss_kmeans_clustering(
		C.ulong(dims),                 // d dimension of the data
		C.ulong(rowCnt),               // n nb of training vectors
		C.ulong(f.clusterCnt),         // k nb of output centroids
		(*C.float)(&vectorFlat[0]),    // x training set (size n * d)
		(*C.float)(&centroidsFlat[0]), // centroids output centroids (size k * d)
		(*C.float)(&qError),           // q_error final quantization error
		//@return error code
	)
	//if c != 0 {
	//	return nil, getLastError()
	//}

	if qError <= 0 {
		//final quantization error
		return nil, errors.New("final quantization error >0")
	}

	centroids = make([][]float64, f.clusterCnt)
	for r := 0; r < f.clusterCnt; r++ {
		float32s := centroidsFlat[int64(r)*dims : int64(r+1)*dims]
		centroids[r] = vecf32ToVecf64(float32s)
	}
	return
}

func vecf32ToVecf64(input []float32) []float64 {
	output := make([]float64, len(input))
	for i, v := range input {
		output[i] = float64(v)
	}
	return output
}

func vecf64ToVecf32(input [][]float64) [][]float32 {
	output := make([][]float32, len(input))
	for i, v := range input {
		output[i] = make([]float32, len(v))
		for j, v2 := range v {
			output[i][j] = float32(v2)
		}
	}
	return output
}

func (f *Faiss) SSE() float64 {
	return 0
}
