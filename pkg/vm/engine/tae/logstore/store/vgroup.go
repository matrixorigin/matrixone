// Copyright 2021 Matrix Origin
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

package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type VGroup interface { //append(vfile) ckp(by what) compact/iscovered
	Log(any) error
	OnCheckpoint(any) //ckp info
	IsCovered(c *compactor) bool
	MergeCheckpointInfo(c *compactor) //only commit group
	PrepareMerge(c *compactor)

	IsCheckpointGroup() bool
	IsUncommitGroup() bool
	IsCommitGroup() bool

	String() string
}

type compactor struct {
	//tid-cid map
	//partial ckp
	//ckp ranges
	checkpointed   map[uint32]uint64
	tidCidMap      map[uint32]map[uint64]uint64
	tidCidMapMu    *sync.RWMutex
	ckpInfoVersion int
}

func newCompactor(base *syncBase) *compactor {
	c := &compactor{
		checkpointed: make(map[uint32]uint64),
	}
	base.checkpointed.RWMutex.RLock()
	for group, ckped := range base.checkpointed.ids {
		c.checkpointed[group] = ckped
	}
	base.checkpointed.RWMutex.RUnlock()
	c.tidCidMap = base.tidLsnMaps
	c.tidCidMapMu = base.tidLsnMapmu
	c.ckpInfoVersion = int(atomic.LoadUint64(&base.syncedVersion))
	return c
}

type baseGroup struct {
	groupID uint32
	vInfo   *vInfo //get groupbyid
	// addrs     map[uint64]int //gid-lsn
}

func newbaseGroup(v *vInfo, gid uint32) *baseGroup {
	return &baseGroup{
		vInfo:   v,
		groupID: gid,
	}
}
func (g *baseGroup) IsCovered(c *compactor)           {}
func (g *baseGroup) MergeCheckpointInfo(c *compactor) {}
func (g *baseGroup) String() string                   { return "" }
func (g *baseGroup) PrepareMerge(c *compactor)        {}

type partialCkpInfo struct {
	size uint32
	ckps *roaring.Bitmap
}

func newPartialCkpInfo(size uint32) *partialCkpInfo {
	return &partialCkpInfo{
		ckps: &roaring.Bitmap{},
		size: size,
	}
}

func (info *partialCkpInfo) String() string {
	return fmt.Sprintf("%s/%d", info.ckps.String(), info.size)
}

func (info *partialCkpInfo) IsAllCheckpointed() bool {
	return info.size == uint32(info.ckps.GetCardinality())
}

func (info *partialCkpInfo) MergePartialCkpInfo(o *partialCkpInfo) {
	if info.size != o.size {
		panic("logic error")
	}
	info.ckps.Or(o.ckps)
}

func (info *partialCkpInfo) MergeCommandInfos(cmds *entry.CommandInfo) {
	if info.size != cmds.Size {
		panic("logic error")
	}
	for _, csn := range cmds.CommandIds {
		info.ckps.Add(csn)
	}
}

func (info *partialCkpInfo) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, info.size); err != nil {
		return
	}
	n += 4
	ckpsn, err := info.ckps.WriteTo(w)
	n += ckpsn
	if err != nil {
		return
	}
	return
}

func (info *partialCkpInfo) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &info.size); err != nil {
		return
	}
	n += 4
	info.ckps = roaring.New()
	ckpsn, err := info.ckps.ReadFrom(r)
	n += ckpsn
	if err != nil {
		return
	}
	return
}

type commitGroup struct {
	*baseGroup
	ckps       *common.ClosedIntervals //ckpped
	Commits    *common.ClosedIntervals
	tidCidMap  map[uint64]uint64
	partialCkp map[uint64]*partialCkpInfo
}

func newcommitGroup(v *vInfo, gid uint32) *commitGroup {
	return &commitGroup{
		baseGroup:  newbaseGroup(v, gid),
		ckps:       common.NewClosedIntervals(),
		tidCidMap:  make(map[uint64]uint64),
		partialCkp: make(map[uint64]*partialCkpInfo),
	}
}

func (g *commitGroup) String() string {
	s := fmt.Sprintf("G%dCommit[ckp-%s,commits-%s,tc-{", g.groupID, g.ckps, g.Commits)
	for tid, cid := range g.tidCidMap {
		s = fmt.Sprintf("%s%d-%d,", s, tid, cid)
	}
	s = fmt.Sprintf("%s},partial-{", s)
	for tid, cmd := range g.partialCkp {
		s = fmt.Sprintf("%s%d-%v/%d,", s, tid, cmd.ckps, cmd.size)
	}
	s = fmt.Sprintf("%s}]", s)
	return s
}

func (g *commitGroup) Log(info any) error {
	commitInfo := info.(*entry.Info)
	if g.Commits == nil {
		g.Commits = common.NewClosedIntervals()
	}
	g.Commits.TryMerge(*common.NewClosedIntervalsByInt(commitInfo.GroupLSN))
	g.tidCidMap[commitInfo.TxnId] = commitInfo.GroupLSN
	return nil
}

func (g *commitGroup) IsCovered(c *compactor) bool {
	ckp, ok := c.checkpointed[g.groupID]
	if !ok {
		return false
	}
	if g.Commits != nil && !g.Commits.IsCoveredByInt(ckp) {
		return false
	}
	return true
}

func (g *commitGroup) IsCommitGroup() bool {
	return true
}
func (g *commitGroup) PrepareMerge(c *compactor) {
	//tid cid map
	gMap, ok := c.tidCidMap[g.groupID]
	if !ok {
		gMap = make(map[uint64]uint64)
	}
	for tid, cid := range g.tidCidMap {
		gMap[tid] = cid
	}
	c.tidCidMap[g.groupID] = gMap
}
func (g *commitGroup) MergeCheckpointInfo(c *compactor) {
	//merge partialckp
	// partialMap, ok := c.partialCKP[g.groupID]
	// if !ok {
	// 	partialMap = make(map[uint64]*partialCkpInfo)
	// }
	// gMap := c.tidCidMap[g.groupID]
	// for lsn, commandsInfo := range g.partialCkp {
	// 	partial, ok := partialMap[lsn]
	// 	if !ok {
	// 		partial = newPartialCkpInfo(commandsInfo.size)
	// 	}
	// 	if partial.size != commandsInfo.size {
	// 		panic("logic error")
	// 	}
	// 	partial.ckps.Or(commandsInfo.ckps)
	// 	if partial.ckps.GetCardinality() == uint64(partial.size) {
	// 		if gMap != nil {
	// 			g.ckps.TryMerge(*common.NewClosedIntervalsByInt(lsn))
	// 			delete(partialMap, lsn)
	// 		}
	// 	}
	// 	partialMap[lsn] = partial
	// }
	// c.partialCKP[g.groupID] = partialMap
	// //merge ckps
	// if len(g.ckps.Intervals) == 0 {
	// 	return
	// }
	// if c.gIntervals == nil {
	// 	ret := make(map[uint32]*common.ClosedIntervals)
	// 	ret[g.groupID] = common.NewClosedIntervalsByIntervals(g.ckps)
	// 	c.gIntervals = ret
	// 	return
	// }
	// _, ok = c.gIntervals[g.groupID]
	// if !ok {
	// 	c.gIntervals[g.groupID] = &common.ClosedIntervals{}
	// }
	// c.gIntervals[g.groupID].TryMerge(*g.ckps)
}

func (g *commitGroup) IsCheckpointGroup() bool {
	return false
}
func (g *commitGroup) IsUncommitGroup() bool {
	return false
}
func (g *commitGroup) OnCheckpoint(info any) {
	ranges := info.(*entry.CkpRanges)
	if ranges.Ranges != nil {
		g.ckps.TryMerge(*ranges.Ranges)
	}
	for lsn, command := range ranges.Command {
		commandinfo, ok := g.partialCkp[lsn]
		if !ok {
			commandinfo = newPartialCkpInfo(command.Size)
		}
		for _, cmd := range command.CommandIds {
			commandinfo.ckps.Add(cmd)
		}
		g.partialCkp[lsn] = commandinfo
	}
}

type uncommitGroup struct {
	*baseGroup
	//uncheckpointed gid-tids//-commands
	//Only support single txn
	UncommitTxn map[uint64]*entry.Tid
}

func newuncommitGroup(v *vInfo, gid uint32) *uncommitGroup {
	return &uncommitGroup{
		baseGroup:   newbaseGroup(v, gid),
		UncommitTxn: make(map[uint64]*entry.Tid),
	}
}
func (g *uncommitGroup) String() string {
	s := "Uncommit["
	for gid, tids := range g.UncommitTxn {
		s = fmt.Sprintf("%s%d-%v", s, gid, tids)
	}
	s = fmt.Sprintf("%s]", s)
	return s
}
func (g *uncommitGroup) OnCheckpoint(any) {} //calculate ckp when compact
func (g *uncommitGroup) IsCovered(c *compactor) bool {
	c.tidCidMapMu.RLock()
	defer c.tidCidMapMu.RUnlock()
	for _, gidTid := range g.UncommitTxn {
		tidMap, ok := c.tidCidMap[gidTid.Group]
		if !ok {
			return false
		}
		ckp, ok := c.checkpointed[gidTid.Group]
		if !ok {
			return false
		}
		lsn, ok := tidMap[gidTid.Tid]
		if !ok {
			return false
		}
		if lsn > ckp {
			return false
		}
	}
	return true
}

func (g *uncommitGroup) MergeCheckpointInfo(c *compactor) {
}

func (g *uncommitGroup) IsCheckpointGroup() bool {
	return false
}
func (g *uncommitGroup) IsUncommitGroup() bool {
	return true
}
func (g *uncommitGroup) IsCommitGroup() bool {
	return false
}
func (g *uncommitGroup) Log(info any) error {
	uncommitInfo := info.(*entry.Info)
	g.UncommitTxn[uncommitInfo.GroupLSN] = &uncommitInfo.Uncommits[0]
	return nil
}

type checkpointGroup struct {
	//gid,lsn ->
	*baseGroup
}

func newcheckpointGroup(v *vInfo, gid uint32) *checkpointGroup {
	return &checkpointGroup{
		baseGroup: newbaseGroup(v, gid),
	}
}
func (g *checkpointGroup) OnCheckpoint(any) {} //ckp info
func (g *checkpointGroup) IsCovered(c *compactor) bool {
	//TODO: not compact ckp entry with payload
	return g.vInfo.vf.Id() <= c.ckpInfoVersion
}
func (g *checkpointGroup) MergeCheckpointInfo(c *compactor) {}
func (g *checkpointGroup) IsCheckpointGroup() bool {
	return true
}
func (g *checkpointGroup) IsCommitGroup() bool {
	return false
}
func (g *checkpointGroup) IsUncommitGroup() bool { return false }

func (g *checkpointGroup) Log(info any) error {
	checkpointInfo := info.(*entry.Info)
	for _, interval := range checkpointInfo.Checkpoints {
		group := g.vInfo.getGroupById(interval.Group) //todo new group if not exist
		if group == nil {
			group = newcommitGroup(g.vInfo, interval.Group)
			g.vInfo.groupmu.Lock()
			g.vInfo.groups[interval.Group] = group
			g.vInfo.groupmu.Unlock()
		}
		group.OnCheckpoint(&interval) //todo range -> ckp info
	}
	return nil
}
func (g *checkpointGroup) String() string {
	s := "Ckp"
	return s
}
