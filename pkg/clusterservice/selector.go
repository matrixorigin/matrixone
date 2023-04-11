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
func (s Selector) SelectByLabel(labels map[string]string, op Op) Selector {
	s.byLabel = true
	s.labelOp = op
	s.labels = labels
	return s
}

func (s Selector) filterCN(cn metadata.CNService) bool {
	return s.filter(cn.ServiceID, cn.Labels)
}

func (s Selector) filterDN(dn metadata.DNService) bool {
	return s.filter(dn.ServiceID, dn.Labels)
}

func (s Selector) filter(serviceID string, labels map[string]metadata.LabelList) bool {
	if s.byServiceID {
		return serviceID == s.serviceID
	}
	if s.byLabel {
		switch s.labelOp {
		case EQ:
			if s.emptyLabel() && len(labels) > 0 {
				return false
			}
			// If it is an empty CN server, return true.
			if len(labels) == 0 {
				return true
			}
			for k, v := range s.labels {
				values, ok := labels[k]
				if !ok {
					return false
				}
				if !containLabel(values.Labels, v) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	return true
}

func (s Selector) emptyLabel() bool {
	if s.labels == nil || len(s.labels) == 0 {
		return true
	}
	return false
}

func containLabel(labels []string, label string) bool {
	for _, l := range labels {
		if l == label {
			return true
		}
	}
	return false
}
