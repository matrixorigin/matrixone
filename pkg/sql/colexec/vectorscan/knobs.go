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

package vectorscan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Session variables that default the BY RANK search knobs.
const (
	SessionVarITopK     = "vector_itopk"
	SessionVarMaxITopK  = "vector_max_itopk"
	SessionVarMaxNprobe = "vector_max_nprobe"
)

// ResolveVariable reads a session variable, as process.Process.GetResolveVariableFunc.
type ResolveVariable func(name string, isSystemVar, isGlobalVar bool) (any, error)

// StampSessionKnobs fills the unset knobs of spec from the session variables. The
// coordinator stamps the copy of spec it sends to a CN that runs the scan without the
// session.
func StampSessionKnobs(spec *plan.IndexSearchScan, resolve ResolveVariable) error {
	if spec == nil || resolve == nil {
		return nil
	}
	for _, knob := range []struct {
		name string
		dst  *uint64
	}{
		{SessionVarITopK, &spec.InternalTopk},
		{SessionVarMaxITopK, &spec.MaxInternalTopk},
		{SessionVarMaxNprobe, &spec.MaxNprobe},
	} {
		if *knob.dst != 0 {
			continue
		}
		v, err := resolve(knob.name, true, false)
		if err != nil {
			return err
		}
		if v == nil {
			continue
		}
		n, ok := v.(int64)
		if !ok || n < 0 {
			return moerr.NewInternalErrorNoCtxf("session variable %s is not a non-negative integer", knob.name)
		}
		*knob.dst = uint64(n)
	}
	return nil
}

// ApplySearchKnobs sets the internal top-k, its cap and the IVF probe cap of req from
// spec, and the knobs spec leaves unset from the session variables through resolve
// (nil where the scan runs without the session). A set internal top-k is the candidate
// budget, lowered to its cap and raised to the result limit.
func ApplySearchKnobs(req *searchplugin.Request, spec *plan.IndexSearchScan, resolve ResolveVariable) error {
	knobs := &plan.IndexSearchScan{
		InternalTopk:    spec.GetInternalTopk(),
		MaxInternalTopk: spec.GetMaxInternalTopk(),
		MaxNprobe:       spec.GetMaxNprobe(),
	}
	if err := StampSessionKnobs(knobs, resolve); err != nil {
		return err
	}
	itopk, maxITopK := knobs.InternalTopk, knobs.MaxInternalTopk
	if maxITopK != 0 && itopk > maxITopK {
		itopk = maxITopK
	}
	req.InternalTopK, req.MaxInternalTopK, req.MaxNprobe = itopk, maxITopK, knobs.MaxNprobe
	if itopk != 0 {
		req.CandidateBudget = max(itopk, req.ResultLimit)
	}
	return nil
}
