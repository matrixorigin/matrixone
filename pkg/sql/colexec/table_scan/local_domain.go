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
}

func (t *TableScan) localDomainColumn(name string) int {
	index := -1
	for i, attr := range t.Attrs {
		if strings.EqualFold(attr, name) {
			if index >= 0 {
				return -1
			}
			index = i
		}
	}
	return index
}

func (t *TableScan) prepareLocalDomains(proc *process.Process) error {
	t.ctr.domainsReady, t.ctr.domainEmpty = false, false
	for _, binding := range t.LocalDomainBuilds {
		column := t.localDomainColumn(binding.Column)
		if column < 0 || column >= len(t.Types) || t.Types[column].Id != int32(binding.Domain.KeyType()) {
			binding.Domain.Disable()
			continue
		}
		producer, err := binding.Domain.Begin(proc, int(t.ParallelID), max(1, int(t.GetMaxParallel())))
		if err != nil {
			return err
		}
		if !binding.Domain.Enabled() {
			producer.Complete(true)
			continue
		}
		t.ctr.domainSources = append(t.ctr.domainSources, localDomainSourceState{producer: producer, column: column})
	}
	for _, binding := range t.LocalDomainProbes {
		column := t.localDomainColumn(binding.Column)
		if column < 0 || column >= len(t.Types) || t.Types[column].Id != int32(binding.Domain.KeyType()) {
			binding.Domain.Disable()
			continue
		}
		if err := binding.Domain.PrepareProbe(proc); err != nil {
			return err
		}
		if !binding.Domain.Enabled() {
			continue
		}
		t.ctr.domainProbes = append(t.ctr.domainProbes, localDomainProbeState{
			domain: binding.Domain, column: column, mask: make([]uint8, runtimefilter.LocalDomainBatchRows),
		})
	}
	if len(t.ctr.domainProbes) > 0 {
		t.ctr.domainRows = make([]int64, 0, runtimefilter.LocalDomainBatchRows)
	}
	return nil
}

func (t *TableScan) waitLocalDomains(proc *process.Process) error {
	if t.ctr.domainsReady {
		return nil
	}
	for i := range t.ctr.domainProbes {
		p := &t.ctr.domainProbes[i]
		var empty bool
		var err error
		p.bloom, empty, err = p.domain.Wait(proc.Ctx)
		if err != nil {
			return err
		}
		t.ctr.domainEmpty = t.ctr.domainEmpty || empty
		p.pass = p.bloom == nil
	}
	t.ctr.domainsReady = true
	return nil
}

func (t *TableScan) applyLocalDomains(proc *process.Process) error {
	bat := t.ctr.buf
	for i := range t.ctr.domainSources {
		s := &t.ctr.domainSources[i]
		start := time.Now()
		if err := s.producer.AddVector(proc.Ctx, bat.Vecs[s.column], bat.RowCount()); err != nil {
			return err
		}
		s.elapsed += time.Since(start)
		s.rows += int64(bat.RowCount())
	}
	var combined []uint8
	for i := range t.ctr.domainProbes {
		p := &t.ctr.domainProbes[i]
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
	t.ctr.domainRows = t.ctr.domainRows[:0]
	for row, keep := range combined {
		if keep != 0 {
			t.ctr.domainRows = append(t.ctr.domainRows, int64(row))
		}
	}
	if len(t.ctr.domainRows) == bat.RowCount() {
		return nil
	}
	return shrinkPopulatedVecs(bat, t.ctr.domainRows, proc.Mp())
}

func (t *TableScan) releaseLocalDomains() {
	for _, source := range t.ctr.domainSources {
		source.producer.Complete(true)
		if t.OpAnalyzer != nil {
			stats := t.OpAnalyzer.GetOpStats()
			stats.AddExtraStat("RuntimeDomainBuildRows", source.rows)
			stats.AddExtraStat("RuntimeDomainBuildElapsedNs", int64(source.elapsed))
		}
	}
	for _, probe := range t.ctr.domainProbes {
		if t.OpAnalyzer != nil {
			stats := t.OpAnalyzer.GetOpStats()
			stats.AddExtraStat("RuntimeDomainTestedRows", probe.tested)
			stats.AddExtraStat("RuntimeDomainRejectedRows", probe.rejected)
			stats.AddExtraStat("RuntimeDomainBypassedRows", probe.bypassed)
		}
	}
	t.ctr.domainSources, t.ctr.domainProbes, t.ctr.domainRows = nil, nil, nil
	t.LocalDomainBuilds, t.LocalDomainProbes = nil, nil
	t.ctr.domainsReady, t.ctr.domainEmpty = false, false
}
