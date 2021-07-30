package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	cubeconfig "github.com/matrixorigin/matrixcube/config"
	"log"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
	msgChan chan *ChanMessage

	periodOfTimer int
	timerClose    CloseFlag

	persistClose CloseFlag

	ddlDeleteClose CloseFlag

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

	/*
	ddl queue
	kv<epoch,[meta1,meta2,...,]
	meta_i includes table,database,index,etc
	 */
	ddlQueue map[uint64][]*Meta

}

func NewPDCallbackImpl(p int) *PDCallbackImpl {
	return &PDCallbackImpl{
		cluster_epoch: 1,
		serverInfo:                      make(map[uint64]uint64),
		msgChan:                         make(chan *ChanMessage),
		periodOfTimer:                   p,
		epoch_info:                   make(map[uint64]uint64),
		ddlQueue: make(map[uint64][]*Meta),
	}
}

type MsgType int

const (
	MSG_TYPE_SERVER_INFO MsgType = iota+1
	MSG_TYPE_MINI_REM_EPOCH
	MSG_TYPE_CLUSTER_EPOCH
)

type ChanMessage struct {
	tp MsgType
	body []byte
	body2 [][]byte
}

const (
	META_TYPE_TABLE int = iota
	META_TYPE_DATABASE
	META_TYPE_INDEX
)

type Meta struct {
	MtEpoch uint64
	MtType int
	MtId uint64
}

func (m Meta) String() string {
	return fmt.Sprintf("epoch %d type %d id %d",m.MtEpoch,m.MtType,m.MtId)
}

func NewMeta(ep uint64,tp int,id uint64) *Meta {
	return &Meta{
		MtEpoch: ep,
		MtType: tp,
		MtId:   id,
	}
}

var (
	CLUSTER_EPOCH_KEY = []byte("cluster_epoch")
	MINI_REM_EPOCH_KEY = []byte("minimum_removable_epoch")
	SERVER_PREFIX = []byte("server_i")
)

//get all kv from cube
func (pci *PDCallbackImpl) getCustomData(k []byte,v []byte) error  {
	if bytes.HasPrefix(k,CLUSTER_EPOCH_KEY) {
		//get cluster epoch
		pci.cluster_epoch = binary.BigEndian.Uint64(v)
	} else if bytes.HasPrefix(k,MINI_REM_EPOCH_KEY) {
		//get minimum removable epoch
		pci.cluster_minimumRemovableEpoch = binary.BigEndian.Uint64(v)
	} else if bytes.HasPrefix(k,SERVER_PREFIX) {
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

	//TODO:When the cluster runs initially, there is not keys any more.
	//load cluster_epoch
	//load minimumRemovableEpoch
	//load kv<server,maximumRemovableEpoch>
	err := kv.LoadCustomData(math.MaxInt64,pci.getCustomData)
	if err != nil {
		return err
	}

	//start timer for epoch increment
	go pci.IncrementEpochPeriodlyRoutine(pci.periodOfTimer)

	//start persistent worker
	go pci.PersistentWorkerRoutine(pci.msgChan, kv)

	//start delete ddl worker
	go pci.DeleteDDLPeriodicallyRoutine()

	return nil
}

/*
When the node changes from the leader to the follower, the Stop will be executed.
kv : the persistent storage
 */
func (pci *PDCallbackImpl) Stop(kv storage.Storage) error {
	//stop timer
	pci.timerClose.Close()

	//stop persistent worker
	close(pci.msgChan)

	//stop delete ddl worker
	pci.ddlDeleteClose.Close()

	//persist cluster epoch, minimumRemovableEpoch, kv<server,maximumRemovableEpoch>
	var buf [8]byte
	var buf2 [8]byte

	pci.rwlock.Lock()
	defer pci.rwlock.Unlock()

	ce := atomic.LoadUint64(&pci.cluster_epoch)

	//save cluster epoch
	binary.BigEndian.PutUint64(buf[:],ce)
	err := kv.PutCustomData(CLUSTER_EPOCH_KEY,buf[:])
	if err != nil {
		return err
	}

	//save minimumRemovableEpoch
	binary.BigEndian.PutUint64(buf[:],pci.cluster_minimumRemovableEpoch)
	err = kv.PutCustomData(MINI_REM_EPOCH_KEY,buf[:])
	if err != nil {
		return err
	}

	//save kv<server,maximumRemovableEpoch>
	for id,mre := range pci.serverInfo {
		binary.BigEndian.PutUint64(buf[:],id)
		binary.BigEndian.PutUint64(buf2[:],mre)

		var key []byte = nil
		key = append(key,SERVER_PREFIX...)
		key = append(key,buf[:]...)

		err = kv.PutCustomData(key,buf2[:])
		if err != nil {
			return err
		}
	}

	return nil
}

/*
When the leader receives a heartbeat, the HandleHeartbeatReq will be executed.
id : the id of the node,
data : the message that the node sent
kv : the persistent storage
 */
func (pci *PDCallbackImpl) HandleHeartbeatReq(id uint64, data []byte, kv storage.Storage) (responseData []byte, err error){
	pci.rwlock.Lock()
	defer pci.rwlock.Unlock()

	//step 1: set [server,maximumRemovableEpoch]

	maxre := binary.BigEndian.Uint64(data)
	pci.serverInfo[id] = maxre

	//step 2: calc minimumRemovableEpoch
	var minRE uint64 = math.MaxUint64

	/**
	Actually, the membership of the cluster may changed.
	When a new server joins, its initial maximumRemovableEpoch is 0.
	The nodes except the new guy will receive a zero minimumRemovableEpoch.
	It has no side effect.

	When a server leaves,  its info will exist in the server_info in next several epochs.
	It has no side effects also.
	 */
	var b2 [][]byte = nil
	for k,v := range pci.serverInfo {
		minRE = MinUint64(minRE,v)

		k_buf := make([]byte,8)
		v_buf := make([]byte,8)
		binary.BigEndian.PutUint64(k_buf,k)
		binary.BigEndian.PutUint64(v_buf,v)

		b2 = append(b2,k_buf,v_buf)
	}

	pci.cluster_minimumRemovableEpoch = minRE

	//fmt.Printf("node %d maxre %d minRe %d \n",id,maxre,minRE)

	//step 3: put these values into the worker
	pci.msgChan <- &ChanMessage{
		tp:   MSG_TYPE_SERVER_INFO,
		body: nil,
		body2: b2,
	}

	buf := make([]byte,8)
	binary.BigEndian.PutUint64(buf,pci.cluster_minimumRemovableEpoch)
	pci.msgChan <- &ChanMessage{
		tp:   MSG_TYPE_MINI_REM_EPOCH,
		body: buf,
	}

	//step 4: response to the server
	var rsp []byte = make([]byte,16)
	binary.BigEndian.PutUint64(rsp,pci.cluster_epoch)
	binary.BigEndian.PutUint64(rsp[8:],pci.cluster_minimumRemovableEpoch)

	return rsp,nil
}

/**
Timer routine for epoch increment
 */
func (pci *PDCallbackImpl) IncrementEpochPeriodlyRoutine(period int){
	pci.timerClose.Open()

	for pci.timerClose.IsOpened() {
		//step 1: incr cluster_epoch
		ce := atomic.AddUint64(&pci.cluster_epoch,1)

		buf := make([]byte,8)
		binary.BigEndian.PutUint64(buf,ce)

		//step 2: put these values into the worker
		pci.msgChan <- &ChanMessage{
			tp:   MSG_TYPE_CLUSTER_EPOCH,
			body: buf,
		}

		time.Sleep(time.Duration(pci.periodOfTimer) * time.Millisecond)
	}
}

/*
store the message into the kv
 */
func (pci *PDCallbackImpl) PersistentWorkerRoutine(msgChan chan *ChanMessage, kv storage.Storage) {
	pci.persistClose.Open()

	//get the message
	//put the body into kv
	for msg := range pci.msgChan {
		switch msg.tp {
		case MSG_TYPE_CLUSTER_EPOCH:
			//fmt.Printf("cluster epoch %v \n",msg.body)

			err := kv.PutCustomData(CLUSTER_EPOCH_KEY,msg.body)
			if err != nil {
				log.Fatal(err)
			}

		case MSG_TYPE_SERVER_INFO:

			//fmt.Printf("server info %v \n",msg.body2)

			//save kv<server,maximumRemovableEpoch>
			for i := 0; i < len(msg.body2); i += 2 {
				var key []byte = nil
				key = append(key,SERVER_PREFIX...)
				key = append(key,msg.body2[i]...)
				err := kv.PutCustomData(key, msg.body2[i + 1])
				if err != nil {
					log.Fatal(err)
				}
			}
		case MSG_TYPE_MINI_REM_EPOCH:

			//fmt.Printf("minimum removable epoch %v \n",msg.body)

			//save minimumRemovableEpoch
			err := kv.PutCustomData(MINI_REM_EPOCH_KEY,msg.body)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

/**
pd leader start the routine.
 */
func (pci *PDCallbackImpl) DeleteDDLPeriodicallyRoutine () {
	pci.ddlDeleteClose.Open()
	for pci.ddlDeleteClose.IsOpened() {
		//step 1: get ddls marked deleted from the catalog service
		//step 2: delete these ddls
		fmt.Printf("id %d delete ddl \n",pci.Id)
		time.Sleep(10 * time.Second)
	}
}

/*
when the server receives a heartbeat response from the leader, the HandleHeartbeatRsp will be executed.
 */
func (sci *PDCallbackImpl) HandleHeartbeatRsp(data []byte) error {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()

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
		//get all epochs that less server_epoch, because their query_cnt can not be increased anymore.
		//sort them
		var eps Uint64List = nil
		for k := range sci.epoch_info {
			//careful
			if k < sci.server_epoch {
				eps = append(eps,k)
			}

			if sci.epoch_info[k] > 0 {
				fmt.Printf("node %d epoch %d query_cnt %d\n",sci.Id,k,sci.epoch_info[k])
			}
		}

		sort.Sort(eps)

		maxRE := uint64(0)
		//calc the maximumRemovableEpoch until the first non-zero epoch
		for _,k := range eps {
			v,ok := sci.epoch_info[k]
			if !ok{
				continue
			}

			//find first non-zero value,break
			if v != 0 {
				break
			}

			maxRE = MaxUint64(maxRE,k)
		}

		sci.server_maximumRemovableEpoch = maxRE
	}

	if pd_mre > sci.server_minimumRemovableEpoch {
		sci.server_minimumRemovableEpoch = pd_mre
	}

	fmt.Printf("id %d cluster_epoch %d minRE %d \n",sci.Id,cluster_epoch,pd_mre)

	//cluster_epoch goes from 1.
	//epoch 0 is invalid.
	//So anything related to 0 will not be processed.
	//run async drop task
	if sci.server_minimumRemovableEpoch > 0 {
		//get all epochs that <= minimumRemovableEpoch
		var eps Uint64List = nil
		for ep := range sci.epoch_info {
			if ep <= sci.server_minimumRemovableEpoch {
				eps = append(eps,ep)
			}
		}

		//delete these epoch infos
		for _,ep := range eps {
			v := sci.epoch_info[ep]
			if v != 0 {
				panic(fmt.Errorf("query_cnt is not zero in removableEpoch. epoch %d epoch_info %v",ep,sci.epoch_info))
			}

			sci.removeEpochInfoUnsafe(ep)
		}

		/*
		start drop task.
		clean metas in epochs less than minimumRemovableEpoch.
		 */
		var q []*Meta = nil
		for ep := range sci.ddlQueue {
			if ep <= sci.server_minimumRemovableEpoch {
				l := sci.removeEpochMetasUnsafe(ep)
				q = append(q,l...)
			}
		}

		//run async drop task
		go sci.DeleteDDLPermanentlyRoutine(sci.server_epoch, q)
	}

	return nil
}

/*
drop task routine
 */
func (sci *PDCallbackImpl) DeleteDDLPermanentlyRoutine(epoch uint64, q []*Meta) {
	for i := 0 ; i < len(q); i++ {
		if q[i].MtEpoch > epoch {
			panic(fmt.Errorf("remove something in the future. server_epoch %d, meta_epoch %d",epoch,q[i].MtEpoch))
		}
		//call catalog service to do the deletion
		fmt.Printf("id %d delete meta %s\n",sci.Id,*q[i])
	}
}

func (sci *PDCallbackImpl) CollectData() []byte {
	sci.rwlock.RLock()
	defer sci.rwlock.RUnlock()

	var buf []byte = make([]byte,8)
	binary.BigEndian.PutUint64(buf,sci.server_maximumRemovableEpoch)
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

func (sci *PDCallbackImpl) IncQueryCountAtEpoch(ep,qc uint64) (uint64, uint64) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if ep == 0 {
		return 0, 0
	}
	sci.epoch_info[ep] += qc
	return ep, sci.epoch_info[ep]
}

/*
	ep: epoch
	qc: query_cnt

	return:
	epoch
	query_cnt after subtracted
*/
func (sci *PDCallbackImpl) DecQueryCountAtEpoch(ep,qc uint64) (uint64, uint64) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if ep == 0 {
		return 0,0
	}
	sci.epoch_info[ep] -= qc
	return ep, sci.epoch_info[ep]
}

/*
put a meta into the queue.
it will not be persisted.
 */
func (sci *PDCallbackImpl) AddMeta(ep uint64,mt *Meta) {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()

	q,ok := sci.ddlQueue[ep]
	if !ok {
		q = []*Meta{mt}
		sci.ddlQueue[ep] = q
	}else{
		q = append(q,mt)
		sci.ddlQueue[ep] = q
	}
}

//multi-thread unsafe
func (sci *PDCallbackImpl) removeEpochMetasUnsafe(ep uint64) []*Meta {
	q,ok := sci.ddlQueue[ep]
	if !ok {
		return nil
	}else{
		delete(sci.ddlQueue,ep)
		return q
	}
}

//multi-thread unsafe
func (sci *PDCallbackImpl) removeEpochInfoUnsafe(ep uint64) {
	delete(sci.epoch_info,ep)
}

//for test
func (sci *PDCallbackImpl) AddEpochInfo(ep,qc uint64) uint64 {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if sci.server_epoch == 0 {
		return 0
	}
	sci.epoch_info[sci.server_epoch] += qc
	return sci.server_epoch
}

//for test
func (sci *PDCallbackImpl) SetEpochInfo(ep,qc uint64) uint64 {
	sci.rwlock.Lock()
	defer sci.rwlock.Unlock()
	if ep == 0 {
		return 0
	}
	sci.epoch_info[ep] = qc
	return ep
}