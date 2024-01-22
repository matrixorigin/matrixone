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
	"regexp"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// NewSelector return a default selector, this empty selector
// will select all instances. For CN type, it only select the
// ones whose work state is working.
func NewSelector() Selector {
	return Selector{}
}

// NewSelectAll will return all CN services.
func NewSelectAll() Selector {
	return Selector{
		all: true,
	}
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

// SelectWithoutLabel updates the selector by removing the labels.
func (s Selector) SelectWithoutLabel(labels map[string]string) Selector {
	for labelKey, labelValue := range s.labels {
		v, ok := labels[labelKey]
		if ok && v == labelValue {
			delete(s.labels, labelKey)
		}
	}
	return s
}

// LabelNum returns the number of labels in this selector.
func (s Selector) LabelNum() int {
	return len(s.labels)
}

func (s Selector) filterCN(cn metadata.CNService) bool {
	return s.filter(cn.ServiceID, cn.Labels)
}

func (s Selector) filterTN(tn metadata.TNService) bool {
	return s.filter(tn.ServiceID, tn.Labels)
}

func (s Selector) filter(serviceID string, labels map[string]metadata.LabelList) bool {
	if s.byServiceID {
		return serviceID == s.serviceID
	}
	return s.Match(labels)
}

// Match check if @labels match the selector.
// At most case, the selector's labels is subset of @labels
//
// return true, if @labels is empty.
func (s Selector) Match(labels map[string]metadata.LabelList) bool {
	if s.byLabel {
		switch s.labelOp {
		case Contain:
			if s.emptyLabel() && len(labels) > 0 {
				return false
			}
			// If it is an empty CN server, return true.
			if len(labels) == 0 {
				return true
			}
			return labelContains(s.labels, labels, nil)

		case EQ:
			if s.emptyLabel() && len(labels) > 0 {
				return false
			}
			// If it is an empty CN server, return true.
			if len(labels) == 0 {
				return true
			}
			return labelEQ(s.labels, labels)

		case EQ_Globbing:
			if s.emptyLabel() && len(labels) > 0 {
				return false
			}
			// If it is an empty CN server, return true.
			if len(labels) == 0 {
				return true
			}
			return labelEQGlobbing(s.labels, labels)

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

// labelContains checks if the source labels contained by destination labels.
// The source labels are requested from client, and the destination labels are
// defined on CN servers. So this method is used to check if the request
// can be sent to this CN server.
func labelContains(
	src map[string]string, dst map[string]metadata.LabelList, comp func(string, string) bool,
) bool {
	for k, v := range src {
		values, ok := dst[k]
		if !ok {
			return false
		}
		if !containLabel(v, values.Labels, comp) {
			return false
		}
	}
	return true
}

func labelEQ(src map[string]string, dst map[string]metadata.LabelList) bool {
	l1 := len(src)
	l2 := len(dst)
	if l1 != l2 {
		return false
	}
	return labelContains(src, dst, nil)
}

func labelEQGlobbing(src map[string]string, dst map[string]metadata.LabelList) bool {
	l1 := len(src)
	l2 := len(dst)
	if l1 != l2 {
		return false
	}
	return labelContains(src, dst, globbing)
}

func containLabel(src string, dst []string, comp func(string, string) bool) bool {
	if comp == nil {
		comp = strings.EqualFold
	}
	for _, l := range dst {
		if comp(src, l) {
			return true
		}
	}
	return false
}

func globbing(src, dst string) bool {
	if dst == "" {
		return false
	}
	// dst is '*', or starts with '*'.
	if strings.HasPrefix(dst, "*") {
		dst = "." + dst
	} else {
		for i := 1; i < len(dst); i++ {
			if dst[i] == '*' && dst[i-1] != '.' {
				dst = dst[:i] + "." + dst[i:]
			}
		}
	}
	if dst[0] != '^' {
		dst = "^" + dst
	}
	if dst[len(dst)-1] != '$' {
		dst = dst + "$"
	}
	ok, err := regexp.MatchString(dst, src)
	if err != nil {
		return false
	}
	return ok
}
