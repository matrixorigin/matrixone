// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_scan

import (
	"math"
	"slices"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type localDomainSourceState struct {
	producer *runtimefilter.LocalDomainSource
	column   int
	rows     int64
	elapsed  time.Duration
}

type localDomainProbeState struct {
	domain   *runtimefilter.LocalDomain
	bloom    *bloomfilter.CBloomFilter
	column   int
	mask     []uint8
	tested   int64
	rejected int64
	bypassed int64
	pass     bool
	ndv      float64
	reason   runtimefilter.LocalDomainPassReason
	snapshot runtimefilter.LocalDomainSnapshot
}

func (tableScan *TableScan) localDomainColumn(name string) int {
	index := -1
	for i, attr := range tableScan.Attrs {
		if strings.EqualFold(attr, name) {
			if index >= 0 {
				return -1
			}
			index = i
		}
	}
	return index
}

func (tableScan *TableScan) prepareLocalDomains(proc *process.Process) error {
	tableScan.ctr.domainsReady, tableScan.ctr.domainEmpty = false, false
	for _, binding := range tableScan.LocalDomainBuilds {
		column := tableScan.localDomainColumn(binding.Column)
		if column < 0 || column >= len(tableScan.Types) || tableScan.Types[column].Id != int32(binding.Domain.KeyType()) {
			binding.Domain.Disable()
			continue
		}
		producer, err := binding.Domain.Begin(proc, int(tableScan.ParallelID), max(1, int(tableScan.GetMaxParallel())))
		if err != nil {
			return err
		}
		if !binding.Domain.Enabled() {
			tableScan.recordLocalDomainSnapshot(binding.Domain.Snapshot(), 0)
			producer.Complete(true)
			continue
		}
		tableScan.ctr.domainSources = append(tableScan.ctr.domainSources, localDomainSourceState{producer: producer, column: column})
	}
	for _, binding := range tableScan.LocalDomainProbes {
		if binding.ProbeNDV <= 1 || math.IsNaN(binding.ProbeNDV) || math.IsInf(binding.ProbeNDV, 0) {
			if tableScan.ParallelID == 0 {
				tableScan.OpAnalyzer.GetOpStats().AddExtraStat("RuntimeDomainInvalidProbeNDV", 1)
			}
			continue
		}
		column := tableScan.localDomainColumn(binding.Column)
		if column < 0 || column >= len(tableScan.Types) || tableScan.Types[column].Id != int32(binding.Domain.KeyType()) {
			binding.Domain.Disable()
			continue
		}
		if err := binding.Domain.PrepareProbe(proc); err != nil {
			return err
		}
		if !binding.Domain.Enabled() {
			tableScan.recordLocalDomainSnapshot(binding.Domain.Snapshot(), binding.ProbeNDV)
			continue
		}
		tableScan.ctr.domainProbes = append(tableScan.ctr.domainProbes, localDomainProbeState{
			domain: binding.Domain, column: column, ndv: binding.ProbeNDV, mask: make([]uint8, runtimefilter.LocalDomainBatchRows),
		})
	}
	if len(tableScan.ctr.domainProbes) > 0 {
		tableScan.ctr.domainRows = make([]int64, 0, runtimefilter.LocalDomainBatchRows)
	}
	return nil
}

func (tableScan *TableScan) waitLocalDomains(proc *process.Process) error {
	if tableScan.ctr.domainsReady {
		return nil
	}
	for i := range tableScan.ctr.domainProbes {
		p := &tableScan.ctr.domainProbes[i]
		var empty bool
		var err error
		p.bloom, empty, err = p.domain.Wait(proc.Ctx)
		if err != nil {
			return err
		}
		p.snapshot = p.domain.Snapshot()
		p.reason = p.snapshot.PassReason
		if p.bloom != nil && !p.domain.ProbeEligible(p.ndv) {
			p.bloom, empty = nil, false
			p.reason = runtimefilter.LocalDomainUnprofitable
		}
		tableScan.ctr.domainEmpty = tableScan.ctr.domainEmpty || empty
		p.pass = p.bloom == nil
	}
	tableScan.ctr.domainsReady = true
	return nil
}

func (tableScan *TableScan) applyLocalDomains(proc *process.Process) error {
	bat := tableScan.ctr.buf
	for i := range tableScan.ctr.domainSources {
		s := &tableScan.ctr.domainSources[i]
		start := time.Now()
		if err := tableScan.withFlatDomainKey(proc, bat.Vecs[s.column], func(key *vector.Vector) error {
			return s.producer.AddVector(proc.Ctx, key, bat.RowCount())
		}); err != nil {
			return err
		}
		s.elapsed += time.Since(start)
		s.rows += int64(bat.RowCount())
	}
	if tableScan.ctr.membershipApplied {
		return nil
	}
	rows, all, err := tableScan.selectLocalDomains(proc, bat)
	tableScan.retireReaderMembership(proc)
	if err != nil || all {
		return err
	}
	if len(rows) == bat.RowCount() {
		return nil
	}
	if tableScan.ctr.dictionaryBoundary.AllowsDictionaryOutput() && len(bat.ExtraBuf) == 0 {
		next, _, err := colexec.SelectBatchColumns(bat, nil, rows, proc.Mp(), tableScan.OutputAllocation, true)
		if err != nil {
			return err
		}
		bat.Clean(proc.Mp())
		tableScan.ctr.buf = next
		return nil
	}
	return shrinkPopulatedVecs(bat, rows, proc.Mp())
}

// selectLocalDomains returns a borrowed mapping without mutating payload. The
// reader consumes it before invoking another predicate callback.
func (tableScan *TableScan) selectLocalDomains(proc *process.Process, bat *batch.Batch) ([]int64, bool, error) {
	var combined []uint8
	var first *localDomainProbeState
	for i := range tableScan.ctr.domainProbes {
		p := &tableScan.ctr.domainProbes[i]
		if p.pass || bat.RowCount() > len(p.mask) {
			p.bypassed += int64(bat.RowCount())
			continue
		}
		if first != nil {
			// A second active domain needs the first domain's own count
			// before AND changes its mask. With one domain, count and collect
			// selected rows together in the final pass instead.
			rejected := int64(0)
			for _, keep := range combined {
				if keep == 0 {
					rejected++
				}
			}
			first.recordRejections(rejected)
			first = nil
		}
		vec := bat.Vecs[p.column]
		matched := false
		if vec.GetType().Oid == p.domain.KeyType() && ((!vec.IsConst() && vec.Length() == bat.RowCount()) || vec.IsConst() && vec.Length() > 0) {
			if err := tableScan.withFlatDomainKey(proc, vec, func(key *vector.Vector) error {
				matched = p.bloom.TestFixedVectorInto(key, p.mask)
				return nil
			}); err != nil {
				return nil, false, err
			}
		}
		if !matched {
			p.pass = true
			p.bypassed += int64(bat.RowCount())
			continue
		}
		mask := p.mask[:bat.RowCount()]
		if vec.IsConst() && vec.Length() != bat.RowCount() && len(mask) > 0 {
			value := mask[0]
			for row := range mask {
				mask[row] = value
			}
		}
		p.tested += int64(len(mask))
		// The unchanged Bloom API already clears NULLs (including const NULL)
		// and broadcasts a constant over its declared logical vector length.
		if combined == nil {
			combined, first = mask, p
		} else {
			rejected := int64(0)
			for row, keep := range mask {
				if keep == 0 {
					rejected++
				}
				combined[row] &= keep
			}
			p.recordRejections(rejected)
		}
	}
	if combined == nil {
		return nil, true, nil
	}
	tableScan.ctr.domainRows = tableScan.ctr.domainRows[:0]
	if first != nil {
		rejected := int64(0)
		for row, keep := range combined {
			if keep != 0 {
				tableScan.ctr.domainRows = append(tableScan.ctr.domainRows, int64(row))
			} else {
				rejected++
			}
		}
		first.recordRejections(rejected)
	} else {
		for row, keep := range combined {
			if keep != 0 {
				tableScan.ctr.domainRows = append(tableScan.ctr.domainRows, int64(row))
			}
		}
	}
	return tableScan.ctr.domainRows, len(tableScan.ctr.domainRows) == bat.RowCount(), nil
}

func (p *localDomainProbeState) recordRejections(rejected int64) {
	p.rejected += rejected
	if p.tested >= runtimefilter.LocalDomainBatchRows && p.rejected*10 < p.tested {
		p.pass = true
	}
}

func (tableScan *TableScan) prepareReaderMembership(proc *process.Process) {
	request := tableScan.ctr.readerFilter
	if request == nil || request.Select == nil || len(tableScan.ctr.domainProbes) == 0 ||
		len(tableScan.ctr.allFilterExecutors) == 0 || tableScan.ctr.padCharToFullLength {
		return
	}
	needed := make([]bool, len(tableScan.Types))
	for _, exprs := range [][]*plan.Expr{tableScan.RuntimeFilterExprs, tableScan.FilterExprs} {
		for _, expr := range exprs {
			if !CollectFilterColumnPositions(expr, needed) {
				return
			}
		}
	}
	predicateColumns := make([]int, 0, len(tableScan.Types))
	for column, used := range needed {
		if used {
			predicateColumns = append(predicateColumns, column)
		}
	}
	var membershipColumns []int
	for _, probe := range tableScan.ctr.domainProbes {
		if !probe.pass {
			membershipColumns = append(membershipColumns, probe.column)
			needed[probe.column] = true
		}
	}
	if len(membershipColumns) == 0 {
		return
	}
	slices.Sort(membershipColumns)
	request.Membership = &engine.ReaderMembershipFilter{
		Columns: slices.Compact(membershipColumns), PredicateColumns: predicateColumns,
		// Runtime predicates are installed before Prepare; validate the actual
		// executable chain here rather than relying on earlier logical filters.
		PredicatesTotal: plan2.LocalDomainPredicatesAreTotal(tableScan.FilterExprs) && plan2.LocalDomainPredicatesAreTotal(tableScan.RuntimeFilterExprs),
	}
	tableScan.ctr.earlyColumns = tableScan.ctr.earlyColumns[:0]
	tableScan.ctr.lateColumns = tableScan.ctr.lateColumns[:0]
	for column := range needed {
		if needed[column] {
			tableScan.ctr.earlyColumns = append(tableScan.ctr.earlyColumns, column)
		} else {
			tableScan.ctr.lateColumns = append(tableScan.ctr.lateColumns, column)
		}
	}
	// Membership changes publication policy; refresh the cached projection proof.
	tableScan.ctr.columnUsesReady = false
	request.Membership.Select = func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
		started := time.Now()
		defer func() { tableScan.ctr.filterActiveDuration += time.Since(started) }()
		if request.Exact != nil || !tableScan.ctr.filterReadMetrics {
			tableScan.recordFilterInput(bat, loaded)
		} else {
			tableScan.recordMembershipKeyInput(bat, loaded, request.Membership.PredicateColumns)
		}
		rows, all, err := tableScan.selectLocalDomains(proc, bat)
		if err != nil {
			return engine.ReaderFilterResult{}, err
		}
		tableScan.ctr.membershipApplied = true
		request.Membership.InputRows += int64(bat.RowCount())
		if all {
			request.Membership.OutputRows += int64(bat.RowCount())
		} else {
			request.Membership.OutputRows += int64(len(rows))
		}
		return engine.ReaderFilterResult{Sels: rows, All: all}, nil
	}
}

// retireReaderMembership runs after ReadWithFilter, including empty results.
// No reader callback or borrowed map is live at this boundary.
func (tableScan *TableScan) retireReaderMembership(proc *process.Process) {
	request := tableScan.ctr.readerFilter
	if request == nil || request.Membership == nil {
		return
	}
	bypass := request.Membership.Bypass
	active := false
	for i := range tableScan.ctr.domainProbes {
		if bypass {
			tableScan.ctr.domainProbes[i].pass = true
		}
		active = active || !tableScan.ctr.domainProbes[i].pass
	}
	if active {
		return
	}
	tableScan.recordReaderMembershipStats()
	if bypass && tableScan.OpAnalyzer != nil {
		tableScan.OpAnalyzer.GetOpStats().AddExtraStat("RuntimeDomainReaderCapacityFallback", 1)
	}
	request.FreeMembership(proc.Mp())
	request.Membership = nil
	tableScan.ctr.columnUsesReady = false
	tableScan.configureLateMaterialization()
	if len(tableScan.ctr.earlyColumns) == 0 {
		for column := range tableScan.Types {
			tableScan.ctr.earlyColumns = append(tableScan.ctr.earlyColumns, column)
		}
	}
}

func (tableScan *TableScan) recordReaderMembershipStats() {
	request := tableScan.ctr.readerFilter
	if request == nil || request.Membership == nil || request.Membership.InputRows == 0 {
		return
	}
	if tableScan.OpAnalyzer != nil {
		stats := tableScan.OpAnalyzer.GetOpStats()
		stats.AddExtraStat("RuntimeDomainReaderInputRows", request.Membership.InputRows)
		stats.AddExtraStat("RuntimeDomainReaderSurvivingRows", request.Membership.OutputRows)
	}
	request.Membership.InputRows, request.Membership.OutputRows = 0, 0
}

func (tableScan *TableScan) releaseLocalDomains() {
	tableScan.recordReaderMembershipStats()
	for _, source := range tableScan.ctr.domainSources {
		source.producer.Complete(true)
		if tableScan.OpAnalyzer != nil {
			stats := tableScan.OpAnalyzer.GetOpStats()
			stats.AddExtraStat("RuntimeDomainBuildRows", source.rows)
			stats.AddExtraStat("RuntimeDomainBuildElapsedNs", int64(source.elapsed))
		}
	}
	for _, probe := range tableScan.ctr.domainProbes {
		if tableScan.OpAnalyzer != nil {
			tableScan.recordLocalDomainSnapshot(probe.snapshot, probe.ndv)
			stats := tableScan.OpAnalyzer.GetOpStats()
			if tableScan.ParallelID == 0 && probe.reason == runtimefilter.LocalDomainUnprofitable {
				stats.AddExtraStat("RuntimeDomainProbeUnprofitable", 1)
			}
			stats.AddExtraStat("RuntimeDomainTestedRows", probe.tested)
			stats.AddExtraStat("RuntimeDomainRejectedRows", probe.rejected)
			stats.AddExtraStat("RuntimeDomainBypassedRows", probe.bypassed)
		}
	}
	tableScan.ctr.domainSources, tableScan.ctr.domainProbes, tableScan.ctr.domainRows = nil, nil, nil
	tableScan.LocalDomainBuilds, tableScan.LocalDomainProbes = nil, nil
	tableScan.ctr.domainsReady, tableScan.ctr.domainEmpty = false, false
}

func (tableScan *TableScan) recordLocalDomainSnapshot(snapshot runtimefilter.LocalDomainSnapshot, ndv float64) {
	if tableScan.ParallelID != 0 || tableScan.OpAnalyzer == nil {
		return
	}
	stats := tableScan.OpAnalyzer.GetOpStats()
	stats.AddExtraStat("RuntimeDomainEstimatedBuildRows", snapshot.EstimatedRows)
	stats.AddExtraStat("RuntimeDomainObservedBuildKeys", snapshot.ObservedRows)
	stats.AddExtraStat("RuntimeDomainFilterBytes", int64(snapshot.FilterBytes))
	stats.AddExtraStat("RuntimeDomainPeakReservedBytes", int64(snapshot.PeakBytes))
	stats.AddExtraStat("RuntimeDomainExpectedSources", int64(snapshot.Expected))
	stats.AddExtraStat("RuntimeDomainFinishedSources", int64(snapshot.Finished))
	stats.AddExtraStat("RuntimeDomainPassReason", int64(snapshot.PassReason))
	if ndv > 0 {
		value := int64(math.MaxInt64)
		if ndv < float64(math.MaxInt64) {
			value = int64(ndv)
		}
		stats.AddExtraStat("RuntimeDomainProbeNDV", value)
	}
}

// Native Bloom APIs require contiguous keys. Normalize only the needed key,
// rather than flattening all selected payload columns at this internal boundary.
func (tableScan *TableScan) withFlatDomainKey(proc *process.Process, key *vector.Vector, consume func(*vector.Vector) error) error {
	if !key.IsDictionary() {
		return consume(key)
	}
	flat, err := key.DupOffHeapWithAllocation(proc.Mp(), tableScan.OutputAllocation)
	if err != nil {
		return err
	}
	defer flat.Free(proc.Mp())
	return consume(flat)
}
