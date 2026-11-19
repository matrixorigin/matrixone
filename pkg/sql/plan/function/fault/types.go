// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fj

import (
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

type serviceType string

const (
	tn  serviceType = "DN"
	cn  serviceType = "CN"
	all serviceType = "ALL"
)

func unmarshalResp(command string, payload []byte, res *PodResponse) {
	switch command {
	case fault.ListFault:
		if err := jsoniter.Unmarshal(payload, &res.ReturnList); err != nil {
			res.ErrorStr = string(payload)
		}
	default:
		str := string(payload)
		if strings.Contains(str, "error") {
			res.ErrorStr = str
		} else {
			res.ReturnStr = string(payload)
		}
	}
}
