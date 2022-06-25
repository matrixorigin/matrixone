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

package frontend

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	cubeconfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

/*
	PD callback control block
*/
type PDCallbackImpl struct {
	cubeconfig.StoreHeartbeatDataProcessor
	/*
		pd structure
	*/

	Id int

	rwlock sync.RWMutex

	cluster_epoch                 uint64
	cluster_minimumRemovableEpoch uint64

	//kv<server,maximumRemovableEpoch>
	serverInfo map[uint64]uint64

	/*
		chan may be blocked.
		temporal scheme
	*/

	//ms
	periodOfTimer int
	timerClose    CloseFlag

	//second
	periodOfDDLDelete int
	ddlDeleteClose    CloseFlag

	/*
		server structure
	*/
	server_epoch uint64
	//from the pd leader
	server_minimumRemovableEpoch uint64

	//calculated after the cluster epoch changed
	server_maximumRemovableEpoch uint64
	//<epoch,query_cnt>
	epoch_info map[uint64]uint64
	//<epoch,ddl_cnt>
	ddl_info map[uint64]uint64
	//timeout of heartbeat response
	heartbeatTimeout *Timeout
	//the interface to remove epoch from catalog service
	removeEpoch func(epoch uint64)

	/*
		ddl queue
		kv<epoch,[meta1,meta2,...,]
		meta_i includes table,database,index,etc
	*/
	ddlQueue map[uint64][]*Meta

	/*
		enableLogging
	*/
	enableLog bool

	/*
		close
	*/
	closeOnce sync.Once

	/*
		the limit of cube load
	*/
	limitOfCubeLoad int
}

/*
NewPDCallbackImpl
*/
func NewPDCallbackImpl(pu *PDCallbackParameterUnit) *PDCallbackImpl {
	return &PDCallbackImpl{
		cluster_epoch:     1,
		serverInfo:        make(map[uint64]uint64),
		periodOfTimer:     pu.timerPeriod,
		periodOfDDLDelete: pu.ddlDeletePeriod,
		epoch_info:        make(map[uint64]uint64),
		ddl_info:          make(map[uint64]uint64),
		ddlQueue:          make(map[uint64][]*Meta),
		heartbeatTimeout:  NewTimeout(time.Duration(pu.heartbeatTimeout)*time.Second, false),
		removeEpoch:       nil,
		enableLog:         pu.enableLog,
		limitOfCubeLoad:   pu.limitOfCubeLoad,
	}
}

type PDCallbackParameterUnit struct {
	/*
		the period of the epoch timer.
		Second
	*/
	timerPeriod int

	/*
		the period of the persistence.
		Second
	*/
	persistencePeriod int

	/*
		the period of the ddl delete.
		Second
	*/
	ddlDeletePeriod int

	/*
		the timeout of heartbeat.
		Second
	*/
	heartbeatTimeout int

	/*
		the limit of cube load
	*/
	limitOfCubeLoad int

	/*
		enable logging
	*/
	enableLog bool
}

/*
tp : the period of the epoch timer. Second
pp : the period of the persistence. Second
ddp : 	the period of the ddl delete. Second
ht : 	the timeout of heartbeat. Second
*/
func NewPDCallbackParameterUnit(tp, pp, ddp, ht int, enableLog bool, lt int) *PDCallbackParameterUnit {
	return &PDCallbackParameterUnit{
		timerPeriod:       tp,
		persistencePeriod: pp,
		ddlDeletePeriod:   ddp,
		heartbeatTimeout:  ht,
		enableLog:         enableLog,
		limitOfCubeLoad:   lt,
	}
}

const (
	META_TYPE_TABLE int = iota
	META_TYPE_DATABASE
	META_TYPE_INDEX
)

type Meta struct {
	MtEpoch uint64
	MtType  int
	MtId    uint64
}

func (m Meta) String() string {
	return fmt.Sprintf("epoch %d type %d id %d", m.MtEpoch, m.MtType, m.MtId)
}

func NewMeta(ep uint64, tp int, id uint64) *Meta {
	return &Meta{
		MtEpoch: ep,
		MtType:  tp,
		MtId:    id,
	}
}

var (
	CLUSTER_EPOCH_KEY  = []byte("cluster_epoch")
	MINI_REM_EPOCH_KEY = []byte("minimum_removable_epoch")
	SERVER_PREFIX      = []byte("server_i")
)

//get all kv from cube
func (pci *PDCallbackImpl) getCustomData(k []byte, v []byte) error {
	if bytes.HasPrefix(k, CLUSTER_EPOCH_KEY) {
		//get cluster epoch
		ce := binary.BigEndian.Uint64(v)
		atomic.StoreUint64(&pci.cluster_epoch, ce)
	} else if bytes.HasPrefix(k, MINI_REM_EPOCH_KEY) {
		//get minimum removable epoch
		pci.cluster_minimumRemovableEpoch = binary.BigEndian.Uint64(v)
	} else if bytes.HasPrefix(k, SERVER_PREFIX) {
		//get <server,maximumRemovableEpoch>
		serverId := binary.BigEndian.Uint64(k[len(SERVER_PREFIX):])
		maxRE := binary.BigEndian.Uint64(v)
		pci.serverInfo[serverId] = maxRE
	} else {
		return fmt.Errorf("unsupported k in getCustomData")
	}
	return nil
}

/*
When the node changes from the follower to the leader, the Start will be executed.
kv : the persistent storage
*/
func (pci *PDCallbackImpl) Start(kv storage.Storage) error {
	pci.rwlock.Lock()
	defer pci.rwlock.Unlock()
	if pci.enableLog {
		logutil.Infof("-------PDC Start enter\n")
		defer logutil.Infof("-------PDC Start exit\n")
	}
	//TODO:When the cluster runs initially, there is not keys any more.

	//start timer for epoch increment
	go pci.IncrementEpochPeriodlyRoutine(pci.periodOfTimer)

	//start delete ddl worker
	go pci.DeleteDDLPeriodicallyRoutine()

	return nil
}

/*
When the node changes from the leader to the follower, the Stop will be executed.
kv : the persistent storage
*/
func (pci *PDCallbackImpl) Stop(kv storage.Storage) error {
	if pci.enableLog {
		logutil.Infof("-------PDC Stop enter\n")
		defer logutil.Infof("-------PDC Stop exit\n")
	}
	pci.rwlock.Lock()
	defer pci.rwlock.Unlock()
	if pci.enableLog {
		logutil.Infof("-------PDC Stop Get Lock\n")
	}

	//stop timer
	pci.timerClose.Close()

	//stop persistent worker
	/*
		Do not close chan twice.
	*/
	if pci.enableLog {
		logutil.Infof("-------PDC Stop close channel\n")
	}

	if pci.enableLog {
		logutil.Infof("-------PDC Stop close channel done\n")
	}
	//stop delete ddl worker
	pci.ddlDeleteClose.Close()

	return nil
}

/*
When the leader receives a heartbeat, the HandleHeartbeatReq will be executed.
id : the id of the node,
data : the message that the node sent
kv : the persistent storage
*/
func (pci *PDCallbackImpl) HandleHeartbeatReq(id uint64, data []byte, kv storage.Storage) (responseData []byte, err error) {
	if pci.enableLog {
		logutil.Infof("-------PDC HandleHeartbeatReq enter\n")
		defer logutil.Infof("-------PDC HandleHeartbeatReq exit\n")
	}
	pci.rwlock.Lock()
	defer pci.rwlock.Unlock()
	//logutil.Infof("%d leader receive heartbeat from %d \n",pci.Id,id)
	//step 1: set [server,maximumRemovableEpoch]
	if pci.enableLog {
		logutil.Infof("-------PDC HandleHeartbeatReq Get Lock\n")
	}
	maxre := binary.BigEndian.Uint64(data)
	pci.serverInfo[id] = maxre

	//step 2: calc minimumRemovableEpoch
	var minRE uint64 = math.MaxUint64

	/*
		TODO: performance optimization
		the minimumRemovableEpoch can be evaluated asynchronously
	*/
	/**
	Actually, the membership of the cluster may changed.
	When a new server joins, its initial maximumRemovableEpoch is 0.
	The nodes except the new guy will receive a zero minimumRemovableEpoch.
	It has no side effect.

	When a server leaves,  its info will exist in the server_info in next several epochs.
	It has no side effects also.
	*/

	for _, v := range pci.serverInfo {
		minRE = MinUint64(minRE, v)
	}

	pci.cluster_minimumRemovableEpoch = minRE

	//logutil.Infof("node %d maxre %d minRe %d \n",id,maxre,minRE)

	//step 4: response to the server
	var rsp []byte = make([]byte, 16)
	ce := atomic.LoadUint64(&pci.cluster_epoch)
	binary.BigEndian.PutUint64(rsp, ce)
	binary.BigEndian.PutUint64(rsp[8:], pci.cluster_minimumRemovableEpoch)

	return rsp, nil
}

/**
Timer routine for epoch increment
*/
func (pci *PDCallbackImpl) IncrementEpochPeriodlyRoutine(period int) {
	pci.timerClose.Open()

	for pci.timerClose.IsOpened() {
		//step 1: incr cluster_epoch
		atomic.AddUint64(&pci.cluster_epoch, 1)

		time.Sleep(time.Duration(pci.periodOfTimer) * time.Second)
	}
}

/**
pd leader start the routine.
*/
func (pci *PDCallbackImpl) DeleteDDLPeriodicallyRoutine() {
	pci.ddlDeleteClose.Open()
	for pci.ddlDeleteClose.IsOpened() {
		//step 1: delete these ddls
		if pci.removeEpoch != nil {
			epoch := pci.GetClusterMinimumRemovableEpoch()
			if epoch > 0 {
				if pci.enableLog {
					logutil.Infof("id %d delete ddl at epoch %d \n", pci.Id, epoch)
				}
				pci.removeEpoch(epoch)
			}
		}

		time.Sleep(time.Duration(pci.periodOfDDLDelete) * time.Second)
	}
}

/*
when the server receives a heartbeat response from the leader, the HandleHeartbeatRsp will be executed.
data : the response from the leader
*/
func (sci *PDCallbackImpl) HandleHeartbeatRsp(data []byte) error {
	if sci.enableLog {
		logutil.Infof("-------PDC HandleHeartbeatRsp enter\n")
		defer logutil.Infof("-------PDC HandleHeartbeatRsp exit\n")
	}
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	sci.heartbeatTimeout.UpdateTime(time.Now())
	//logutil.Infof("time Gap %s \n",time.Since(sci.heartbeatTimeout.lastTime))

	cluster_epoch := binary.BigEndian.Uint64(data)
	pd_mre := binary.BigEndian.Uint64(data[8:])

	/*
		change server_epoch;
		calc maxRemovableEpoch;
	*/
	if cluster_epoch > sci.server_epoch {
		sci.server_epoch = cluster_epoch
		sci.epoch_info[sci.server_epoch] = 0

		//update maxRemovableEpoch
		//get all epochs that less than server_epoch, because their query_cnt can not be increased anymore.
		var eps Uint64List = nil
		for k := range sci.epoch_info {
			//careful
			if k < sci.server_epoch {
				eps = append(eps, k)
			}

			//if sci.epoch_info[k] > 0 {
			//	logutil.Infof("node %d epoch %d query_cnt %d\n",sci.Id,k,sci.epoch_info[k])
			//}
		}

		//sort them ascending
		sort.Sort(eps)

		maxRE := uint64(0)
		//calc the maximumRemovableEpoch until the first non-zero epoch
		for _, k := range eps {
			v, ok := sci.epoch_info[k]
			if !ok {
				continue
			}

			//find first non-zero value,break
			if v != 0 {
				break
			}

			maxRE = MaxUint64(maxRE, k)
		}

		//TODO: atomic set
		sci.server_maximumRemovableEpoch = maxRE
	}

	//if there is no business, then it updates the minimumRemovableEpoch
	sci.server_minimumRemovableEpoch = MinUint64(pd_mre, sci.server_maximumRemovableEpoch)

	//logutil.Infof("id %d cluster_epoch %d minRE %d \n",sci.Id,cluster_epoch,pd_mre)

	//cluster_epoch goes from 1.
	//epoch 0 is invalid.
	//So anything related to 0 will not be processed.
	//run async drop task
	if sci.server_minimumRemovableEpoch > 0 {
		//get all epochs that <= minimumRemovableEpoch
		var eps Uint64List = nil
		for ep := range sci.epoch_info {
			if ep <= sci.server_minimumRemovableEpoch {
				eps = append(eps, ep)
			}
		}

		//delete these epoch infos
		for _, ep := range eps {
			v := sci.epoch_info[ep]
			if v != 0 {
				logutil.Errorf("query_cnt is not zero in removableEpoch. epoch %d epoch_info %v", ep, sci.epoch_info)
				break
			}

			sci.removeEpochInfoUnsafe(ep)
		}

		/*
			if there is a ddl in this epoch, then run async drop task
		*/
		var ddl_ep []uint64 = nil
		var ddl_max_ep uint64 = 0
		for ep, ddlc := range sci.ddl_info {
			if ep <= sci.server_minimumRemovableEpoch {
				ddl_ep = append(ddl_ep, ep)

				if ddlc > 0 {
					ddl_max_ep = MaxUint64(ddl_max_ep, ep)
				}
			}
		}

		for _, ep := range ddl_ep {
			sci.removeDDLInfoUnsafe(ep)
		}

		if ddl_max_ep > 0 {
			go sci.DeleteDDLPermanentlyRoutine(ddl_max_ep)
		}
	}

	return nil
}

/*
drop task routine
less than or equal to the epoch will be deleted.
*/
func (sci *PDCallbackImpl) DeleteDDLPermanentlyRoutine(max_ep uint64) {
	//drive catalog service DeleteDDL
	if sci.removeEpoch != nil && max_ep > 0 {
		if sci.enableLog {
			logutil.Infof("async delete ddl epoch %d \n", max_ep)
		}
		sci.removeEpoch(max_ep)
	}
}

func (sci *PDCallbackImpl) CollectData() []byte {
	sci.rwlock.RLock()
	defer sci.rwlock.RUnlock()

	//TODO: atmoic read
	var buf []byte = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, sci.server_maximumRemovableEpoch)
	//logutil.Infof("%d send heartbeat\n",sci.Id)
	return buf
}

/*
	Add query_cnt to the current epoch.
	Also, it pins the epoch that will not be removed in the maximumRemovableEpoch.
	ep: epoch
	qc: query_cnt

	return:
	epoch
	query_cnt after added
*/
func (sci *PDCallbackImpl) IncQueryCountAtCurrentEpoch(qc uint64) (uint64, uint64) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if sci.server_epoch == 0 {
		return 0, 0
	}
	sci.epoch_info[sci.server_epoch] += qc
	return sci.server_epoch, sci.epoch_info[sci.server_epoch]
}

func (sci *PDCallbackImpl) IncQueryCountAtEpoch(ep, qc uint64) (uint64, uint64) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if ep == 0 {
		return 0, 0
	}
	sci.epoch_info[ep] += qc
	return ep, sci.epoch_info[ep]
}

func (sci *PDCallbackImpl) IncDDLCountAtEpoch(ep, ddlc uint64) (uint64, uint64) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if ep == 0 {
		return 0, 0
	}
	sci.ddl_info[ep] += ddlc
	return ep, sci.ddl_info[ep]
}

/*
	ep: epoch
	qc: query_cnt

	return:
	epoch
	query_cnt after subtracted
*/
func (sci *PDCallbackImpl) DecQueryCountAtEpoch(ep, qc uint64) (uint64, uint64) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if ep == 0 {
		return 0, 0
	}
	sci.epoch_info[ep] -= qc
	return ep, sci.epoch_info[ep]
}

/**
the server has the rights to accept something.
false: if the heartbeat is timeout
true: if the heartbeat is instant
*/
func (sci *PDCallbackImpl) CanAcceptSomething() bool {
	sci.rwlock.RLock()
	defer sci.rwlock.RUnlock()
	return !sci.heartbeatTimeout.isTimeout()
}

func (sci *PDCallbackImpl) SetRemoveEpoch(f func(uint64)) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	sci.removeEpoch = f
}

func (sci *PDCallbackImpl) GetClusterMinimumRemovableEpoch() uint64 {
	sci.rwlock.RLock()
	defer sci.rwlock.RUnlock()
	return sci.cluster_minimumRemovableEpoch
}

/*
put a meta into the queue.
it will not be persisted.
*/
func (sci *PDCallbackImpl) AddMeta(ep uint64, mt *Meta) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()

	q, ok := sci.ddlQueue[ep]
	if !ok {
		q = []*Meta{mt}
		sci.ddlQueue[ep] = q
	} else {
		q = append(q, mt)
		sci.ddlQueue[ep] = q
	}
}

//multi-thread unsafe
func (sci *PDCallbackImpl) removeEpochMetasUnsafe(ep uint64) []*Meta {
	q, ok := sci.ddlQueue[ep]
	if !ok {
		return nil
	} else {
		delete(sci.ddlQueue, ep)
		return q
	}
}

//multi-thread unsafe
func (sci *PDCallbackImpl) removeEpochInfoUnsafe(ep uint64) {
	delete(sci.epoch_info, ep)
}

//multi-thread unsafe
func (sci *PDCallbackImpl) removeDDLInfoUnsafe(ep uint64) {
	delete(sci.ddl_info, ep)
}

//for test
func (sci *PDCallbackImpl) AddEpochInfo(ep, qc uint64) uint64 {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if sci.server_epoch == 0 {
		return 0
	}
	sci.epoch_info[sci.server_epoch] += qc
	return sci.server_epoch
}

//for test
func (sci *PDCallbackImpl) SetEpochInfo(ep, qc uint64) uint64 {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if ep == 0 {
		return 0
	}
	sci.epoch_info[ep] = qc
	return ep
}
