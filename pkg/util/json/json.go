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

package json

import (
	"encoding/json"
	"strconv"

	"github.com/itchyny/gojq"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tidwall/pretty"
)

// MustMarshal must marshal json
func MustMarshal(v interface{}) []byte {
	d, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return d
}

// MustUnmarshal must unmarshal json
func MustUnmarshal(data []byte, v interface{}) {
	if err := json.Unmarshal(data, v); err != nil {
		panic(err)
	}
}

// Pretty pretty json
func Pretty(v interface{}) []byte {
	return pretty.Pretty(MustMarshal(v))
}

// RunJQOnString run jq on data.  If the result is a single value, return the value.
// otherwise, return the array of values.  The input data must be a valid json string.
func RunJQOnString(jq string, data string) (any, error) {
	var v any
	if data != "" {
		err := json.Unmarshal([]byte(data), &v)
		if err != nil {
			return nil, err
		}
	}
	return RunJQOnAny(jq, v)
}

// RunJQOnAny run jq on data.  If the result is a single value, return the value.
// otherwise, return the array of values.  The input data must be a value that
// is unmarshalled from a valid json string.
func RunJQOnAny(jq string, data any) (any, error) {
	if jq == "" {
		return data, nil
	}

	pq, err := gojq.Parse(jq)
	if err != nil {
		return nil, err
	}

	return RunJQ(pq, data)
}

// RunJQ run jq on data.  If the result is a single value, return the value.
// otherwise, return the array of values.
func RunJQ(jq *gojq.Query, data any) (any, error) {
	var res []any
	iter := jq.Run(data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}

		if err, ok := v.(error); ok {
			return nil, err
		}

		res = append(res, v)
	}

	if len(res) == 0 {
		return nil, nil
	} else if len(res) == 1 {
		return res[0], nil
	} else {
		return res, nil
	}
}

// RunJQInt run jq on data and return the int value.
func RunJQInt(jq string, data string) (int, error) {
	res, err := RunJQOnString(jq, data)
	if err != nil {
		return 0, err
	}

	switch v := res.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	default:
		return 0, moerr.NewInvalidInputf(moerr.Context(), "invalid jq result: %v", res)
	}
}

// RunJQInt run jq on data and return the float64 value.
func RunJQFloat(jq string, data string) (float64, error) {
	res, err := RunJQOnString(jq, data)
	if err != nil {
		return 0, err
	}

	switch v := res.(type) {
	case int:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, moerr.NewInvalidInputf(moerr.Context(), "invalid jq result: %v", res)
	}
}
