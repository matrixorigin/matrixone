package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	cubeconfig "github.com/matrixorigin/matrixcube/config"
	"log"
	"math"
	"matrixone/pkg/client"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

/*
	PD callback control block
 */
type PDCallbackImpl struct {
	pconfig.ContainerHeartbeatDataProcessor

	rwlock sync.RWMutex

	cluter_epoch uint64
	minimumRemovableEpoch uint64

	//kv<server,maximumRemovableEpoch>
	serverInfo map[uint64]uint64

	/*
	chan may be blocked.
	temporal scheme
	 */
	msgChan chan *ChanMessage

	periodOfTimer int
	timerClose client.CloseFlag

	persistClose client.CloseFlag
}

func NewPDCallbackImpl(p int) *PDCallbackImpl {
	return &PDCallbackImpl{
		serverInfo:                      make(map[uint64]uint64),
		msgChan:                         make(chan *ChanMessage),
		periodOfTimer:                   p,
	}
}

type ServerCallbackImpl struct {
	cubeconfig.StoreHeartbeatDataProcessor
	server_epoch uint64
	server_minimumRemovableEpoch uint64
	//<epoch,query_cnt>
	epoch_info map[uint64]uint64
}

func NewServerCallbackImpl() *ServerCallbackImpl {
	return &ServerCallbackImpl{
		epoch_info:                   make(map[uint64]uint64),
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

var (
	CLUSTER_EPOCH_KEY = []byte("cluster_epoch")
	MINI_REM_EPOCH_KEY = []byte("minimum_removable_epoch")
	SERVER_PREFIX = []byte("server_i")
)

//get all kv from cube
func (pci *PDCallbackImpl) getCustomData(k []byte,v []byte) error  {
	if bytes.HasPrefix(k,CLUSTER_EPOCH_KEY) {
		//get cluster epoch
		pci.cluter_epoch = binary.BigEndian.Uint64(v)
	} else if bytes.HasPrefix(k,MINI_REM_EPOCH_KEY) {
		//get minimum removable epoch
		pci.minimumRemovableEpoch = binary.BigEndian.Uint64(v)
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
	go pci.PersistentWorkerRoutine(pci.msgChan, nil)

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

	//persist cluster epoch, minimumRemovableEpoch, kv<server,maximumRemovableEpoch>
	var buf [8]byte
	var buf2 [8]byte

	pci.rwlock.Lock()
	defer pci.rwlock.Unlock()

	ce := atomic.LoadUint64(&pci.cluter_epoch)

	//save cluster epoch
	binary.BigEndian.PutUint64(buf[:],ce)
	err := kv.PutCustomData(CLUSTER_EPOCH_KEY,buf[:])
	if err != nil {
		return err
	}

	//save minimumRemovableEpoch
	binary.BigEndian.PutUint64(buf[:],pci.minimumRemovableEpoch)
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
	//TODO: handle memebership change

	maxre := binary.BigEndian.Uint64(data)
	pci.serverInfo[id] = maxre

	//step 2: calc minimumRemovableEpoch
	var minRE uint64 = math.MaxUint64


	var b2 [][]byte = nil
	for k,v := range pci.serverInfo {
		minRE = client.MinUint64(minRE,v)

		k_buf := make([]byte,8)
		v_buf := make([]byte,8)
		binary.BigEndian.PutUint64(k_buf,k)
		binary.BigEndian.PutUint64(v_buf,v)

		b2 = append(b2,k_buf,v_buf)
	}

	pci.minimumRemovableEpoch = minRE

	//step 3: put these values into the worker

	pci.msgChan <- &ChanMessage{
		tp:   MSG_TYPE_SERVER_INFO,
		body: nil,
		body2: b2,
	}

	buf := make([]byte,8)
	binary.BigEndian.PutUint64(buf,pci.minimumRemovableEpoch)
	pci.msgChan <- &ChanMessage{
		tp:   MSG_TYPE_MINI_REM_EPOCH,
		body: buf,
	}

	//step 4: response to the server
	//TODO:
	var rsp []byte = make([]byte,16)
	binary.BigEndian.PutUint64(rsp,pci.cluter_epoch)
	binary.BigEndian.PutUint64(rsp[8:],pci.minimumRemovableEpoch)

	return rsp,nil
}

/**
Timer routine for epoch increment
 */
func (pci *PDCallbackImpl) IncrementEpochPeriodlyRoutine(period int){
	pci.timerClose.Open()

	for pci.timerClose.IsOpened() {
		//step 1: incr cluster_epoch
		ce := atomic.AddUint64(&pci.cluter_epoch,1)

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
			fmt.Printf("cluster epoch \n")

			err := kv.PutCustomData(CLUSTER_EPOCH_KEY,msg.body)
			if err != nil {
				log.Fatal(err)
			}

		case MSG_TYPE_SERVER_INFO:

			fmt.Printf("server info \n")

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

			fmt.Printf("minimum removable epoch \n")

			//save minimumRemovableEpoch
			err := kv.PutCustomData(MINI_REM_EPOCH_KEY,msg.body)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

/*
when the server receives a heartbeat response from the leader, the HandleHeartbeatRsp will be executed.
 */
func (sci *ServerCallbackImpl) HandleHeartbeatRsp(data []byte) error {
	cluster_epoch := binary.BigEndian.Uint64(data)
	pd_mre := binary.BigEndian.Uint64(data[8:])
	if cluster_epoch > sci.server_epoch {
		sci.server_epoch = cluster_epoch
	}

	if pd_mre > sci.server_minimumRemovableEpoch {
		sci.server_minimumRemovableEpoch = pd_mre
	}

	//TODO:clear epoch <= minimumRemovableEpoch
	//TODO:start drop task
	fmt.Println("drop task")
	return nil
}

func (sci *ServerCallbackImpl) CollectData() []byte {
	//get all epochs, sort them
	var keys client.Uint64List= nil
	for k,_ := range sci.epoch_info {
		keys = append(keys,k)
	}

	sort.Sort(keys)

	maxRE := uint64(0)
	//calc the maximumRemovableEpoch until the first non-zero epoch
	for _,k := range keys {
		v,ok := sci.epoch_info[k]
		if !ok{
			continue
		}

		//find first non-zero value,break
		if v != 0 {
			break
		}
		maxRE = client.MaxUint64(maxRE,v)
	}

	var buf []byte = make([]byte,8)
	binary.BigEndian.PutUint64(buf,maxRE)
	return buf
}