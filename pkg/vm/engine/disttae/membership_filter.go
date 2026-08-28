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
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func isNilMembershipFilter(filter docfilter.MembershipFilter) bool {
	if filter == nil {
		return true
	}
	value := reflect.ValueOf(filter)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

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
		filter, ok := hint.BF.(docfilter.MembershipFilter)
		if !ok {
			return engine.FilterHint{}, nil, false, moerr.NewInvalidInputNoCtxf(
				"membership filter %T cannot be shared between readers", hint.BF,
			)
		}
		if isNilMembershipFilter(filter) || !filter.Valid() {
			return engine.FilterHint{}, nil, false, moerr.NewInvalidInputNoCtx(
				"membership filter is nil or invalid",
			)
		}
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

// buildReadersWithMembershipFilter centralizes the partial-construction
// ownership contract for a set of reader shards. Any source returned by
// buildSource belongs to this helper; buildSource has not consumed the current
// filter share when it returns an error. buildReader, like readutil.NewReader,
// consumes both source and hint.BF on every return.
func buildReadersWithMembershipFilter(
	readers []engine.Reader,
	readerCount int,
	preparedHint engine.FilterHint,
	mainFilter docfilter.MembershipFilter,
	buildSource func(int) (engine.DataSource, error),
	buildReader func(engine.DataSource, engine.FilterHint) (engine.Reader, error),
) ([]engine.Reader, error) {
	if readerCount < 0 || buildSource == nil || buildReader == nil {
		closeReaders(readers)
		return nil, moerr.NewInvalidInputNoCtx(
			"reader construction requires a non-negative count and builders",
		)
	}
	if mainFilter != nil && (isNilMembershipFilter(mainFilter) || !mainFilter.Valid()) {
		closeReaders(readers)
		return nil, moerr.NewInvalidInputNoCtx(
			"reader construction received a nil or invalid membership filter",
		)
	}
	if mainFilter == nil && preparedHint.BF != nil {
		closeReaders(readers)
		return nil, moerr.NewInvalidInputNoCtx(
			"reader construction has an unowned membership filter",
		)
	}
	for i := 0; i < readerCount; i++ {
		hint := preparedHint
		var readerFilter docfilter.MembershipFilter
		if mainFilter != nil {
			readerFilter = mainFilter.Share()
			if isNilMembershipFilter(readerFilter) || !readerFilter.Valid() {
				if !isNilMembershipFilter(readerFilter) {
					readerFilter.Free()
				}
				closeReaders(readers)
				return nil, moerr.NewInternalErrorNoCtx(
					"membership filter returned a nil or invalid reader share",
				)
			}
			hint.BF = readerFilter
		}

		source, err := buildSource(i)
		if err != nil {
			if source != nil {
				source.Close()
			}
			if readerFilter != nil {
				readerFilter.Free()
			}
			closeReaders(readers)
			return nil, err
		}

		reader, err := buildReader(source, hint)
		if err != nil {
			// buildReader owns the current source and filter share even on
			// failure. Only completed readers remain ours to close here.
			closeReaders(readers)
			return nil, err
		}
		readers = append(readers, reader)
	}
	return readers, nil
}
