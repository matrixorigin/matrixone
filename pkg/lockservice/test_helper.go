package lockservice

import (
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap/zapcore"
)

var (
	testSockets = "unix:///tmp/lockservice.sock"
)

// RunLockServicesForTest is used to start a lock table allocator and some
// lock services for test
func RunLockServicesForTest(
	level zapcore.Level,
	serviceIDs []string,
	lockTableBindTimeout time.Duration,
	fn func(LockTableAllocator, []LockService),
	adjustConfig func(*Config)) {

	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntimeWithLevel(level))
	if err := os.RemoveAll(testSockets[:7]); err != nil {
		panic(err)
	}

	allocator := NewLockTableAllocator(testSockets, lockTableBindTimeout, morpc.Config{})
	services := make([]LockService, 0, len(serviceIDs))

	cns := make([]metadata.CNService, 0, len(serviceIDs))
	configs := make([]Config, 0, len(serviceIDs))
	for _, v := range serviceIDs {
		address := fmt.Sprintf("unix:///tmp/service-%s.sock", v)
		if err := os.RemoveAll(address[7:]); err != nil {
			panic(err)
		}
		cns = append(cns, metadata.CNService{
			ServiceID:          v,
			LockServiceAddress: address,
		})
		configs = append(configs, Config{ServiceID: v, ListenAddress: address})
	}
	cluster := clusterservice.NewMOCluster(
		nil,
		0,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			cns,
			[]metadata.DNService{
				{
					LockServiceAddress: testSockets,
				},
			}))
	runtime.ProcessLevelRuntime().SetGlobalVariables(runtime.ClusterService, cluster)

	for _, cfg := range configs {
		if adjustConfig != nil {
			adjustConfig(&cfg)
		}
		services = append(services,
			NewLockService(cfg).(*service))
	}
	fn(allocator.(*lockTableAllocator), services)

	for _, s := range services {
		if err := s.Close(); err != nil {
			panic(err)
		}
	}
	if err := allocator.Close(); err != nil {
		panic(err)
	}
}

// WaitWaiters wait waiters
func WaitWaiters(
	ls LockService,
	table uint64,
	key []byte,
	waitersCount int) error {
	s := ls.(*service)
	v, err := s.getLockTable(table)
	if err != nil {
		return err
	}

	lb := v.(*localLockTable)
	lb.mu.Lock()
	lock, ok := lb.mu.store.Get(key)
	if !ok {
		panic("missing lock")
	}
	lb.mu.Unlock()
	for {
		if lock.waiter.waiters.len() == waitersCount {
			return nil
		}
		time.Sleep(time.Millisecond * 10)
	}
}
