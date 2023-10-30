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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans/elkans"
	"strconv"
	"strings"
)

const (
	defaultKmeansMaxIteration   = 500
	defaultKmeansDeltaThreshold = 0.01
	defaultKmeansDistanceType   = kmeans.L2
	defaultKmeansInitType       = kmeans.Random
	defaultKmeansClusterCnt     = 1

	configSeparator     = ","
	arrayGroupSeparator = "|"
)

var (
	distTypeStrToEnum map[string]kmeans.DistanceType
	initTypeStrToEnum map[string]kmeans.InitType
)

func init() {
	distTypeStrToEnum = map[string]kmeans.DistanceType{
		"L2":     kmeans.L2,
		"IP":     kmeans.InnerProduct,
		"COSINE": kmeans.CosineDistance,
	}

	initTypeStrToEnum = map[string]kmeans.InitType{
		"RANDOM":   kmeans.Random,
		"KMEANS++": kmeans.KmeansPlusPlus,
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
func NewAggClusterCenters(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, config any, _ any) (agg.Agg[any], error) {
	aggPriv := &sAggClusterCenters{}

	var err error
	aggPriv.clusterCnt, aggPriv.distType, aggPriv.initType, err = decodeConfig(config)
	if err != nil {
		return nil, err
	}

	switch inputTypes[0].Oid {
	case types.T_array_float32, types.T_array_float64:
		aggPriv.arrType = inputTypes[0]
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
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
	initType   kmeans.InitType

	// arrType is the type of the array/vector
	// It is used while converting array/vector from []byte to []float64 or []float32
	arrType types.Type
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

	getArrays := func(bytes []byte, arrDim int64) [][]byte {
		var arrays [][]byte

		for i := int64(0); i < int64(len(bytes)); i += arrDim {
			end := i + arrDim
			if end > int64(len(bytes)) {
				end = int64(len(bytes))
			}
			arrays = append(arrays, bytes[i:end])
		}
		return arrays
	}

	// values would be having list of vectors/arrays combined as one []byte
	//TODO: need to verify with @m-schen
	arrays := getArrays(values, int64(len(values))/count)
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
	result := make([][]byte, 0, len(s.groupedData))

	// Run kmeans logic on each group.
	for _, arrGroup := range s.groupedData {
		if len(arrGroup) == 0 {
			// if there is only no element in the group, then we skip it.
			continue
		}

		if len(arrGroup) == 1 {
			// if there is only one element in the group, then that element is the centroid.
			jsonData, err := json.Marshal(arrGroup)
			if err != nil {
				return nil, err
			}
			result = append(result, jsonData)
			continue
		}

		// 1. convert [][]byte to [][]float64
		var vecf64List [][]float64
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

		// 2. run kmeans
		clusterer, err := elkans.NewKMeans(vecf64List,
			int(s.clusterCnt),
			defaultKmeansMaxIteration,
			defaultKmeansDeltaThreshold,
			s.distType, s.initType)
		if err != nil {
			return nil, err
		}

		centers, err := clusterer.Cluster()
		if err != nil {
			return nil, err
		}

		// 3. convert centroids (ie [][]float64) to `json array` string
		jsonStr, err := json.Marshal(centers)
		if err != nil {
			return nil, err
		}

		// 4. add the json-string byte[] to result
		result = append(result, jsonStr)
	}

	return result, nil
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

	// 4. initType
	var initType = uint16(s.initType)
	buf.Write(types.EncodeUint16(&initType))

	// 5. groupedData
	strList := make([]string, 0, len(s.groupedData))
	for i := range s.groupedData {
		strList = append(strList, s.arraysToString(s.groupedData[i]))
	}
	buf.Write(types.EncodeStringSlice(strList))

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

	// 4. initType
	s.initType = kmeans.InitType(types.DecodeUint16(data[:2]))
	data = data[2:]

	// 4. groupedData
	var err error
	strList := types.DecodeStringSlice(data)
	s.groupedData = make([][][]byte, len(strList))
	for i := range s.groupedData {
		s.groupedData[i], err = s.stringToArrays(strList[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// arraysToString converts list of array/vector to string
// e.g. []vector -> "1,2,3|4,5,6|"
func (s *sAggClusterCenters) arraysToString(arrays [][]byte) string {
	var res string
	var commaSeperatedArrString string
	for _, arr := range arrays {
		switch s.arrType.Oid {
		case types.T_array_float32:
			commaSeperatedArrString = types.BytesToArrayToString[float32](arr)
		case types.T_array_float64:
			commaSeperatedArrString = types.BytesToArrayToString[float64](arr)
		}
		res += commaSeperatedArrString + arrayGroupSeparator
	}
	return res

}

// stringToArrays converts string to a list of array/vector
// e.g. "1,2,3|4,5,6|" -> []array
func (s *sAggClusterCenters) stringToArrays(str string) ([][]byte, error) {
	var res [][]byte
	var vecf []byte
	var err error

	arrays := strings.Split(str, arrayGroupSeparator)
	for _, arr := range arrays {
		if len(strings.TrimSpace(arr)) == 0 {
			continue
		}
		switch s.arrType.Oid {
		case types.T_array_float32:
			vecf, err = types.StringToArrayToBytes[float32](arr)
			if err != nil {
				return nil, err
			}
		case types.T_array_float64:
			vecf, err = types.StringToArrayToBytes[float64](arr)
			if err != nil {
				return nil, err
			}
		}
		res = append(res, vecf)
	}
	return res, nil

}

func decodeConfig(config any) (k uint64, distType kmeans.DistanceType, initType kmeans.InitType, err error) {
	bts, ok := config.([]byte)
	if ok && bts != nil {
		commaSeperatedConfigStr := string(bts)
		configs := strings.Split(commaSeperatedConfigStr, configSeparator)

		parseK := func(v string) (uint64, error) {
			return strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		}

		parseDistType := func(v string) (kmeans.DistanceType, error) {
			if res, ok := distTypeStrToEnum[v]; !ok {
				return 0, moerr.NewInternalErrorNoCtx("unsupported distance_type '%s' for cluster_centers", v)
			} else {
				return res, nil
			}
		}

		parseInitType := func(v string) (kmeans.InitType, error) {
			if res, ok := initTypeStrToEnum[v]; !ok {
				return 0, moerr.NewInternalErrorNoCtx("unsupported init_type '%s' for cluster_centers", v)
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
			case 2:
				initType, err = parseInitType(configs[i])
			}
			if err != nil {
				return 0, defaultKmeansDistanceType, defaultKmeansInitType, err
			}
		}
		return k, distType, initType, nil

	}
	return defaultKmeansClusterCnt, defaultKmeansDistanceType, defaultKmeansInitType, nil
}
