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

package clusterservice

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// NewSelector return a default selector, this empty selector
// will select all input
func NewSelector() Selector {
	return Selector{}
}

// NewServiceIDSelector
func NewServiceIDSelector(serviceID string) Selector {
	return NewSelector().SelectByServiceID(serviceID)
}

// SelectByServiceID select service by service ID
func (s Selector) SelectByServiceID(serviceID string) Selector {
	s.byServiceID = true
	s.serviceID = serviceID
	return s
}

// SelectByLabel select service by label
func (s Selector) SelectByLabel(labelName string, op Op, values []string) Selector {
	if len(values) == 0 {
		panic("invalid expect values")
	}

	s.byLabel = true
	s.labelOp = op
	s.labelName = labelName
	s.labelValues = values
	return s
}

func (s Selector) filterCN(cn metadata.CNService) bool {
	return s.filter(cn.ServiceID, cn.Labels)
}

func (s Selector) filterDN(dn metadata.DNService) bool {
	return s.filter(dn.ServiceID, dn.Labels)
}

func (s Selector) filter(serviceID string, labels map[string]string) bool {
	if s.byServiceID {
		return serviceID == s.serviceID
	}
	if s.byLabel {
		switch s.labelOp {
		case EQ:
			return labels != nil &&
				s.labelValues[0] == labels[s.labelName]
		default:
			return false
		}
	}
	return true
}
