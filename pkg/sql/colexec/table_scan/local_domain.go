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
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
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
		if err := s.producer.AddVector(proc.Ctx, bat.Vecs[s.column], bat.RowCount()); err != nil {
			return err
		}
		s.elapsed += time.Since(start)
		s.rows += int64(bat.RowCount())
	}
	var combined []uint8
	for i := range tableScan.ctr.domainProbes {
		p := &tableScan.ctr.domainProbes[i]
		if p.pass || bat.RowCount() > len(p.mask) {
			p.bypassed += int64(bat.RowCount())
			continue
		}
		vec := bat.Vecs[p.column]
		if vec.GetType().Oid != p.domain.KeyType() || (!vec.IsConst() && vec.Length() != bat.RowCount()) || (vec.IsConst() && vec.Length() == 0) ||
			!p.bloom.TestFixedVectorInto(vec, p.mask) {
			p.pass = true
			p.bypassed += int64(bat.RowCount())
			continue
		}
		mask := p.mask[:bat.RowCount()]
		if vec.IsConst() && len(mask) > 0 {
			value := mask[0]
			for row := range mask {
				mask[row] = value
			}
		}
		p.tested += int64(len(mask))
		for row := range mask {
			if vec.IsNull(uint64(row)) {
				mask[row] = 0
			}
			if mask[row] == 0 {
				p.rejected++
			}
		}
		if p.tested >= runtimefilter.LocalDomainBatchRows && p.rejected*10 < p.tested {
			p.pass = true
		}
		if combined == nil {
			combined = mask
		} else {
			for row := range combined {
				combined[row] &= mask[row]
			}
		}
	}
	if combined == nil {
		return nil
	}
	tableScan.ctr.domainRows = tableScan.ctr.domainRows[:0]
	for row, keep := range combined {
		if keep != 0 {
			tableScan.ctr.domainRows = append(tableScan.ctr.domainRows, int64(row))
		}
	}
	if len(tableScan.ctr.domainRows) == bat.RowCount() {
		return nil
	}
	return shrinkPopulatedVecs(bat, tableScan.ctr.domainRows, proc.Mp())
}

func (tableScan *TableScan) releaseLocalDomains() {
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
