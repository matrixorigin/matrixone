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

package functionAgg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans/elkans"
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
)

const (
	defaultKmeansMaxIteration   = 500
	defaultKmeansDeltaThreshold = 0.01
	defaultKmeansDistanceType   = kmeans.CosineDistance
	defaultInitType             = kmeans.Random
	defaultKmeansClusterCnt     = 1

	configSeparator = ","
)

var (
	distTypeStrToEnum map[string]kmeans.DistanceType
)

func init() {
	distTypeStrToEnum = map[string]kmeans.DistanceType{
		"vector_l2_ops":     kmeans.L2Distance,
		"vector_ip_ops":     kmeans.InnerProduct,
		"vector_cosine_ops": kmeans.CosineDistance,
	}
}

var (
	AggClusterCentersSupportedParameters = []types.T{
		types.T_array_float32, types.T_array_float64,
	}

	AggClusterCentersReturnType = func(typs []types.Type) types.Type {
		return types.T_varchar.ToType()
	}
)

// NewAggClusterCenters this agg func will take a vector/array column and run clustering algorithm like kmeans and
// return the 'k' centroids.
func NewAggClusterCenters(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, config any) (agg.Agg[any], error) {
	aggPriv := &sAggClusterCenters{}

	var err error
	aggPriv.clusterCnt, aggPriv.distType, err = decodeConfig(config)
	if err != nil {
		return nil, err
	}

	switch inputTypes[0].Oid {
	case types.T_array_float32, types.T_array_float64:
		aggPriv.arrType = inputTypes[0]
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for cluster_centers", inputTypes[0])
}

type sAggClusterCenters struct {
	// groupedData will hold the list of vectors/arrays. It is a 3D slice because it is a list of groups (based on group by)
	// and each group will have list of vectors/arrays based on agg condition.
	// [group1] -> [array1, array2]
	// [group2] -> [array3, array4, array5]
	// NOTE: here array is []byte ie types.T_array_float32 or types.T_array_float64
	groupedData [][][]byte

	// Kmeans parameters
	clusterCnt uint64
	distType   kmeans.DistanceType

	// arrType is the type of the array/vector
	// It is used while converting array/vector from []byte to []float64 or []float32
	arrType types.Type
}

func (s *sAggClusterCenters) Dup() agg.AggStruct {
	val := &sAggClusterCenters{
		groupedData: make([][][]byte, len(s.groupedData)),
		clusterCnt:  s.clusterCnt,
		distType:    s.distType,
		arrType:     s.arrType,
	}

	//TODO: verify with @ouyuanning
	val.groupedData = deepCopy3DSlice(s.groupedData)

	return val
}

func (s *sAggClusterCenters) Grows(cnt int) {
	// grow the groupedData slice based on the number of groups
	s.groupedData = append(s.groupedData, make([][][]byte, cnt)...)
}
func (s *sAggClusterCenters) Free(_ *mpool.MPool) {}
func (s *sAggClusterCenters) Fill(groupNumber int64, values []byte, lastResult []byte, count int64, isEmpty bool, isNull bool) ([]byte, bool, error) {
	// NOTE: this function is mostly copied from group_concat.go

	if isNull {
		return nil, isEmpty, nil
	}

	splitBytes := func(data []byte, numberOfParts int64) ([][]byte, error) {

		if numberOfParts <= 0 {
			return nil, moerr.NewInternalErrorNoCtx("invalid numberOfParts %v", numberOfParts)
		}

		dataLength := int64(len(data))

		if dataLength%numberOfParts != 0 {
			return nil, moerr.NewInternalErrorNoCtx("data length not multiple of numberOfParts")
		}

		partSize := dataLength / numberOfParts

		var partitions [][]byte
		for start := int64(0); start < dataLength; start += partSize {
			partitions = append(partitions, data[start:start+partSize])
		}
		return partitions, nil
	}

	// values would be having list of vectors/arrays combined as one []byte
	//TODO: need to verify with @m-schen
	arrays, err := splitBytes(values, count)
	if err != nil {
		return nil, isEmpty, err
	}
	s.groupedData[groupNumber] = append(s.groupedData[groupNumber], arrays...)

	return nil, false, nil
}

func (s *sAggClusterCenters) Merge(groupNumber1 int64, groupNumber2 int64, result1 []byte, result2 []byte, isEmpty1 bool, isEmpty2 bool, priv2 any) ([]byte, bool, error) {
	// NOTE: this function is mostly copied from group_concat.go

	if isEmpty2 {
		return nil, isEmpty1 && isEmpty2, nil
	}

	s2 := priv2.(*sAggClusterCenters)
	s.groupedData[groupNumber1] = append(s.groupedData[groupNumber1], s2.groupedData[groupNumber2][:]...)

	return nil, isEmpty1 && isEmpty2, nil
}

func (s *sAggClusterCenters) Eval(lastResult [][]byte) ([][]byte, error) {
	result := make([][]byte, len(s.groupedData))

	// Run kmeans logic on each group.
	for groupId, arrGroup := range s.groupedData {
		if len(arrGroup) == 0 {
			// if there is only no element in the group, then we return empty JSON array
			result[groupId] = util.UnsafeStringToBytes("[]")
			continue
		}

		// 1. convert [][]byte to [][]float64
		vecf64List := s.bytesListToVecF64List(arrGroup)

		// 2. run kmeans
		clusterer, err := elkans.NewKMeans(vecf64List, int(s.clusterCnt), defaultKmeansMaxIteration, defaultKmeansDeltaThreshold, s.distType, defaultInitType)
		if err != nil {
			return nil, err
		}
		centers, err := clusterer.Cluster()
		if err != nil {
			return nil, err
		}

		// 3. convert centroids (ie [][]float64) to json string
		arraysJsonStr, err := s.arraysToString(centers)
		if err != nil {
			return nil, err
		}

		// 4. add the json-string byte[] to result
		result[groupId] = util.UnsafeStringToBytes(arraysJsonStr)
	}

	return result, nil
}

// bytesListToVecF64List converts [][]byte to [][]float64. If [][]byte represent vecf32, then it will
// be casting it to vecf64
func (s *sAggClusterCenters) bytesListToVecF64List(arrGroup [][]byte) (vecf64List [][]float64) {

	for _, arr := range arrGroup {
		switch s.arrType.Oid {
		case types.T_array_float32:
			// 1. convert []byte to []float64
			vecf32 := types.BytesToArray[float32](arr)

			// 1.a cast to []float64
			_vecf64 := make([]float64, len(vecf32))
			for j, v := range vecf32 {
				_vecf64[j] = float64(v)
			}

			vecf64List = append(vecf64List, _vecf64)

		case types.T_array_float64:
			vecf64List = append(vecf64List, types.BytesToArray[float64](arr))
		}
	}
	return vecf64List
}

// arraysToString converts [][]float64 to json string
func (s *sAggClusterCenters) arraysToString(centers [][]float64) (res string, err error) {

	switch s.arrType.Oid {
	case types.T_array_float32:

		// 3.a cast [][]float64 to [][]float32
		_centers := make([][]float32, len(centers))
		for i, center := range centers {
			_centers[i], err = moarray.Cast[float64, float32](center)
			if err != nil {
				return "", err
			}
		}

		// 3.b create json string for [][]float32
		// NOTE: here we can't use jsonMarshall as it does not accept precision as done in ArraysToString
		// We need precision here, as it is the final output that will be printed on SQL console.
		res = fmt.Sprintf("[ %s ]", types.ArraysToString[float32](_centers, ","))

	case types.T_array_float64:

		// 3.c create json string for [][]float64
		res = fmt.Sprintf("[ %s ]", types.ArraysToString[float64](centers, ","))
	}
	return res, nil
}

func (s *sAggClusterCenters) MarshalBinary() ([]byte, error) {

	if len(s.groupedData) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer

	// 1. arrType
	buf.Write(types.EncodeType(&s.arrType))

	// 2. clusterCnt
	buf.Write(types.EncodeUint64(&s.clusterCnt))

	// 3. distType
	var distType = uint16(s.distType)
	buf.Write(types.EncodeUint16(&distType))

	// 4. groupedData
	encoded, err := json.Marshal(s.groupedData)
	if err != nil {
		return nil, err
	}
	buf.Write(encoded)

	return buf.Bytes(), nil
}

func (s *sAggClusterCenters) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	// 1.arrType
	s.arrType = types.DecodeType(data[:types.TSize])
	data = data[types.TSize:]

	// 2. clusterCnt
	s.clusterCnt = types.DecodeUint64(data[:8])
	data = data[8:]

	// 3. distType
	s.distType = kmeans.DistanceType(types.DecodeUint16(data[:2]))
	data = data[2:]

	// 4. groupedData
	err := json.Unmarshal(data, &s.groupedData)
	if err != nil {
		return err
	}
	return nil
}

// decodeConfig will decode the config string (separated by configSeparator) and return the k and distance_type
func decodeConfig(config any) (k uint64, distType kmeans.DistanceType, err error) {
	bts, ok := config.([]byte)
	if ok && bts != nil {
		commaseparatedConfigStr := string(bts)
		configs := strings.Split(commaseparatedConfigStr, configSeparator)

		parseK := func(v string) (uint64, error) {
			return strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		}

		parseDistType := func(v string) (kmeans.DistanceType, error) {
			v = strings.ToLower(v)
			if res, ok := distTypeStrToEnum[v]; !ok {
				return 0, moerr.NewInternalErrorNoCtx("unsupported distance_type '%s' for cluster_centers", v)
			} else {
				return res, nil
			}
		}

		for i := range configs {
			configs[i] = strings.TrimSpace(configs[i])
			switch i {
			case 0:
				k, err = parseK(configs[i])
			case 1:
				distType, err = parseDistType(configs[i])
			}
			if err != nil {
				return 0, defaultKmeansDistanceType, err
			}
		}
		return k, distType, nil

	}
	return defaultKmeansClusterCnt, defaultKmeansDistanceType, nil
}

func deepCopy3DSlice(src [][][]byte) [][][]byte {
	dst := make([][][]byte, len(src))
	for i := range src {
		dst[i] = make([][]byte, len(src[i]))
		for j := range src[i] {
			dst[i][j] = make([]byte, len(src[i][j]))
			copy(dst[i][j], src[i][j])
		}
	}
	return dst
}
