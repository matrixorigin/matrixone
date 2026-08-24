// Copyright 2026 Matrix Origin
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

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// prepareMembershipFilter reconstructs transported bytes once at the outermost
// reader builder. The returned filter is shareable; owned reports whether this
// call created its builder reference and therefore must Free it.
func prepareMembershipFilter(
	hint engine.FilterHint,
	admission docfilter.MemoryAdmission,
) (
	engine.FilterHint,
	docfilter.MembershipFilter,
	bool,
	error,
) {
	if hint.BF != nil {
		hint.MembershipFilterBytes = nil
		filter, _ := hint.BF.(docfilter.MembershipFilter)
		return hint, filter, false, nil
	}
	if len(hint.MembershipFilterBytes) == 0 {
		return hint, nil, false, nil
	}

	filter, err := docfilter.NewWithMemoryAdmission(
		hint.MembershipFilterBytes,
		admission,
	)
	if err != nil {
		return engine.FilterHint{}, nil, false, err
	}
	hint.MembershipFilterBytes = nil
	hint.BF = filter
	return hint, filter, true, nil
}

func membershipFilterAdmissionForProcess(p any) docfilter.MemoryAdmission {
	proc, ok := p.(*process.Process)
	if !ok || proc == nil {
		return nil
	}
	return docfilter.AdmissionForService(proc.GetService())
}
