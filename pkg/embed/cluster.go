// Copyright 2021-2024 Matrix Origin
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

package embed

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil/clusteradmission"
)

type state int

const (
	stopped = state(0)
	started = state(1)
)

var (
	minPort = uint64(10000)
	maxPort = uint64(60000)

	basePortStep  = uint64(20)
	portLeaseSpan = uint64(1000)

	clusterID = getInitValue("mo-test.cluster")
)

const (
	clusterInfrastructurePortBaseCount = uint64(3)
	tnPortBaseCount                    = uint64(1)
	cnPortBaseCount                    = uint64(2)
)

type clusterPortLease struct {
	base uint64
	next atomic.Uint64
	lock *flock.Flock
}

type cluster struct {
	sync.RWMutex

	id       uint64
	state    state
	files    []string
	services []*operator
	startFn  func(*operator) error

	pendingCleanup []*operator
	portLease      *clusterPortLease
	portLeaseBase  uint64
	portLeaseNext  uint64
	testAdmission  *clusteradmission.Lease

	options struct {
		dataPath                    string
		cn                          int
		withProxy                   bool
		preStart                    func(ServiceOperator)
		testing                     bool
		allowConcurrentTestClusters bool
		heartbeatTimeout            time.Duration
		storeTimeout                time.Duration
	}

	ports struct {
		servicePort int
		raftPort    int
		gossipPort  int
	}
}

func NewCluster(
	opts ...Option,
) (Cluster, error) {
	c := &cluster{
		id:      atomic.AddUint64(&clusterID, 1),
		state:   stopped,
		startFn: func(op *operator) error { return op.Start() },
	}
	for _, opt := range opts {
		opt(c)
	}
	if err := c.adjust(); err != nil {
		return nil, err
	}

	if err := c.initConfigs(); err != nil {
		return cleanupClusterOnError(c, err)
	}

	if err := c.createServiceOperators(0); err != nil {
		return cleanupClusterOnError(c, err)
	}
	return c, nil
}

// cleanupClusterOnError preserves the cleanup owner when rollback is not yet
// complete. A non-nil cluster returned with an error is not usable, but its
// caller must retain it and retry Close. Returning nil is safe only after Close
// has proved that every service and both cluster leases were released.
func cleanupClusterOnError(c Cluster, cause error) (Cluster, error) {
	if c == nil {
		return nil, cause
	}
	cleanupErr := c.Close()
	if cleanupErr != nil {
		return c, errors.Join(cause, cleanupErr)
	}
	return nil, cause
}

func (c *cluster) ID() uint64 {
	return c.id
}

func (c *cluster) Start() (err error) {
	c.Lock()
	defer c.Unlock()

	if c.state == started {
		return moerr.NewInvalidStateNoCtx("embed mo cluster already started")
	}
	if err = c.ensurePortLeaseLocked(); err != nil {
		return err
	}

	if c.options.testing {
		if c.testAdmission != nil {
			return moerr.NewInvalidStateNoCtx(
				"embedded test cluster cleanup is incomplete",
			)
		}
		mode := clusteradmission.Exclusive
		if c.options.allowConcurrentTestClusters {
			mode = clusteradmission.AllowConcurrent
		}
		admission, acquireErr := clusteradmission.Acquire(context.Background(), mode)
		if acquireErr != nil {
			return acquireErr
		}
		c.testAdmission = admission
	}

	if err = c.doStartLocked(0); err != nil {
		cleanupErr := c.closeServicesFromLocked(0)
		if cleanupErr == nil {
			cleanupErr = c.releaseTestAdmissionLocked()
		}
		return errors.Join(err, cleanupErr)
	}
	c.state = started
	return nil
}

func (c *cluster) doStartLocked(from int) error {
	var wg sync.WaitGroup
	var startErr atomic.Value
	for _, s := range c.services[from:] {
		if s.serviceType != metadata.ServiceType_CN {
			if err := c.startServiceLocked(s); err != nil {
				return err
			}
			continue
		}

		wg.Add(1)
		go func(s *operator) {
			defer wg.Done()
			if err := c.startServiceLocked(s); err != nil {
				// Only the first error is captured; concurrent failures
				// from other services are discarded since knowing that
				// any service failed is sufficient to abort startup.
				startErr.CompareAndSwap(nil, err)
			}
		}(s)
	}

	wg.Wait()
	if v := startErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (c *cluster) startServiceLocked(op *operator) error {
	if c.startFn != nil {
		return c.startFn(op)
	}
	return op.Start()
}

func (c *cluster) Close() error {
	c.Lock()
	defer c.Unlock()

	err := c.closeServicesLocked()
	if err == nil {
		err = c.releaseTestAdmissionLocked()
	}
	if err == nil {
		err = c.releasePortLeaseLocked()
	}
	return err
}

func (c *cluster) closeServicesLocked() error {
	err := errors.Join(
		c.closeServicesFromLocked(0),
		c.retryPendingCleanupLocked(),
	)
	c.state = stopped
	return err
}

func (c *cluster) closeServicesFromLocked(from int) error {
	var err error
	for i := len(c.services) - 1; i >= from; i-- {
		err = errors.Join(err, c.services[i].Close())
	}
	return err
}

func (c *cluster) GetService(
	sid string,
) (ServiceOperator, error) {
	var v ServiceOperator
	c.ForeachServices(
		func(s ServiceOperator) bool {
			if s.ServiceID() == sid {
				v = s
				return false
			}
			return true
		},
	)

	if v == nil {
		return nil, moerr.NewInvalidStateNoCtx("service not found")
	}

	return v, nil
}

func (c *cluster) ForeachServices(
	fn func(ServiceOperator) bool,
) {
	c.RLock()
	defer c.RUnlock()

	for _, s := range c.services {
		if !fn(s) {
			return
		}
	}
}

func (c *cluster) GetCNService(
	index int,
) (ServiceOperator, error) {
	var v ServiceOperator
	var i int
	c.ForeachServices(
		func(s ServiceOperator) bool {
			if s.ServiceType() == metadata.ServiceType_CN {
				if i == index {
					v = s
					return false
				}
				i++
				return true
			}
			return true
		},
	)

	if v == nil {
		return nil, moerr.NewInvalidStateNoCtx("service not found")
	}

	return v, nil
}

func (c *cluster) StartNewCNService(n int) error {
	c.Lock()
	defer c.Unlock()

	if c.state != started {
		panic("cannot start cn services in stopped cluster")
	}
	if err := c.retryPendingCleanupLocked(); err != nil {
		return err
	}
	if err := c.validateAdditionalCNPortCapacityLocked(n); err != nil {
		return err
	}

	serviceFrom := len(c.services)
	cnFrom := c.options.cn
	c.options.cn += n

	if err := c.initCNConfigs(cnFrom); err != nil {
		return errors.Join(err, c.rollbackNewServicesLocked(serviceFrom, cnFrom))
	}
	if err := c.createServiceOperators(serviceFrom); err != nil {
		return errors.Join(err, c.rollbackNewServicesLocked(serviceFrom, cnFrom))
	}

	if err := c.doStartLocked(serviceFrom); err != nil {
		return errors.Join(err, c.rollbackNewServicesLocked(serviceFrom, cnFrom))
	}
	return nil
}

func (c *cluster) rollbackNewServicesLocked(serviceFrom, cnFrom int) error {
	newServices := append([]*operator(nil), c.services[serviceFrom:]...)
	err := c.closeServicesFromLocked(serviceFrom)
	c.services = c.services[:serviceFrom]
	c.files = c.files[:serviceFrom]
	c.options.cn = cnFrom

	if err != nil {
		for _, op := range newServices {
			if op.needsCleanup() {
				c.pendingCleanup = append(c.pendingCleanup, op)
			}
		}
	}
	return err
}

func (c *cluster) retryPendingCleanupLocked() error {
	pending := c.pendingCleanup
	c.pendingCleanup = nil

	var err error
	for _, op := range pending {
		closeErr := op.Close()
		err = errors.Join(err, closeErr)
		if closeErr != nil && op.needsCleanup() {
			c.pendingCleanup = append(c.pendingCleanup, op)
		}
	}
	return err
}

func (c *cluster) adjust() error {
	if c.options.cn == 0 {
		c.options.cn = 1
	}
	if err := validateInitialCNPortCapacity(c.options.cn); err != nil {
		return err
	}

	createdDataPath := false
	if c.options.dataPath == "" {
		dataPath, err := os.MkdirTemp(os.TempDir(), "mo-cluster-test-")
		if err != nil {
			return err
		}
		c.options.dataPath = dataPath
		createdDataPath = true
	}
	lease, err := acquireClusterPortLease()
	if err != nil {
		if createdDataPath {
			err = errors.Join(err, os.RemoveAll(c.options.dataPath))
			c.options.dataPath = ""
		}
		return err
	}
	c.portLease = lease
	c.portLeaseBase = lease.base
	c.portLeaseNext = lease.base
	cleanup := func(cause error) error {
		cause = errors.Join(cause, c.releasePortLeaseLocked())
		if createdDataPath {
			cause = errors.Join(cause, os.RemoveAll(c.options.dataPath))
			c.options.dataPath = ""
		}
		return cause
	}
	if c.ports.servicePort, err = c.nextBasePort(); err != nil {
		return cleanup(err)
	}
	if c.ports.raftPort, err = c.nextBasePort(); err != nil {
		return cleanup(err)
	}
	if c.ports.gossipPort, err = c.nextBasePort(); err != nil {
		return cleanup(err)
	}
	return nil
}

func (c *cluster) createServiceOperators(from int) error {
	for i, f := range c.files[from:] {
		s, err := newService(
			f,
			i,
			func(o *operator) {
				if o.serviceType == metadata.ServiceType_LOG {
					o.cfg.LogService.UpdateAddresses(
						"127.0.0.1",
						c.ports.servicePort,
						c.ports.raftPort,
						c.ports.gossipPort,
					)
					o.cfg.LogService.UUID = uuid.NewString()
					o.cfg.LogService.BootstrapConfig.InitHAKeeperMembers = []string{"131072:" + o.cfg.LogService.UUID}
				}
			},
			c.options.testing,
		)
		if err != nil {
			return err
		}
		if c.options.heartbeatTimeout > 0 {
			s.Adjust(func(cfg *ServiceConfig) {
				applyHAKeeperHeartbeatTimeout(cfg, s.serviceType, c.options.heartbeatTimeout)
			})
		}
		if c.options.storeTimeout > 0 && s.serviceType == metadata.ServiceType_LOG {
			s.Adjust(func(cfg *ServiceConfig) {
				cfg.LogService.HAKeeperConfig.TNStoreTimeout.Duration = c.options.storeTimeout
				cfg.LogService.HAKeeperConfig.CNStoreTimeout.Duration = c.options.storeTimeout
			})
		}

		if c.options.preStart != nil {
			c.options.preStart(s)
		}
		c.services = append(c.services, s)
	}
	return nil
}

func applyHAKeeperHeartbeatTimeout(
	cfg *ServiceConfig,
	serviceType metadata.ServiceType,
	timeout time.Duration,
) {
	switch serviceType {
	case metadata.ServiceType_CN:
		cfg.CN.HAKeeper.HeatbeatTimeout.Duration = timeout
	case metadata.ServiceType_TN:
		// Keep the legacy [dn] alias effective for callers that still use it.
		if cfg.TN_please_use_getTNServiceConfig == nil {
			cfg.TN_please_use_getTNServiceConfig = cfg.TNCompatible
		}
		if cfg.TN_please_use_getTNServiceConfig != nil {
			cfg.TN_please_use_getTNServiceConfig.HAKeeper.HeatbeatTimeout.Duration = timeout
		}
		if cfg.TNCompatible != nil && cfg.TNCompatible != cfg.TN_please_use_getTNServiceConfig {
			cfg.TNCompatible.HAKeeper.HeatbeatTimeout.Duration = timeout
		}
	}
}

func (c *cluster) initConfigs() error {
	if err := c.initLogServiceConfig(); err != nil {
		return err
	}

	if err := c.initTNServiceConfig(); err != nil {
		return err
	}

	return c.initCNConfigs(0)
}

func (c *cluster) initCNConfigs(from int) error {
	for i := from; i < c.options.cn; i++ {
		file := filepath.Join(c.options.dataPath, fmt.Sprintf("cn-%d.toml", i))
		c.files = append(c.files, file)
		text, err := genConfigText(
			cnConfig,
			templateArgs{
				I:            i,
				ID:           c.id,
				DataDir:      c.options.dataPath,
				ServicePort:  c.ports.servicePort,
				NextBasePort: c.nextBasePort,
			},
		)
		if err != nil {
			return err
		}
		err = genConfig(file, text)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) initLogServiceConfig() error {
	file := filepath.Join(c.options.dataPath, "log.toml")
	c.files = append(c.files, file)
	text, err := genConfigText(
		logConfig,
		templateArgs{
			ID:           c.id,
			DataDir:      c.options.dataPath,
			ServicePort:  c.ports.servicePort,
			NextBasePort: c.nextBasePort,
		},
	)
	if err != nil {
		return err
	}
	return genConfig(file, text)
}

func (c *cluster) initTNServiceConfig() error {
	file := filepath.Join(c.options.dataPath, "tn.toml")
	c.files = append(c.files, file)
	text, err := genConfigText(
		tnConfig,
		templateArgs{
			ID:           c.id,
			DataDir:      c.options.dataPath,
			ServicePort:  c.ports.servicePort,
			NextBasePort: c.nextBasePort,
		},
	)
	if err != nil {
		return err
	}
	return genConfig(file, text)
}

func acquireClusterPortLease() (*clusterPortLease, error) {
	for base := minPort; base+portLeaseSpan <= maxPort; base += portLeaseSpan {
		lease, locked, err := tryAcquireClusterPortLease(base)
		if err != nil {
			return nil, err
		}
		if locked {
			return lease, nil
		}
	}
	return nil, moerr.NewInternalErrorNoCtx("no embedded-test port range is available")
}

func tryAcquireClusterPortLease(base uint64) (*clusterPortLease, bool, error) {
	dir := filepath.Join(os.TempDir(), "mo-test-port-leases")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, false, err
	}
	fl := flock.New(filepath.Join(dir, fmt.Sprintf("%d.lock", base)))
	locked, err := fl.TryLock()
	if err != nil {
		return nil, false, err
	}
	if !locked {
		return nil, false, fl.Close()
	}
	lease := &clusterPortLease{base: base, lock: fl}
	lease.next.Store(base)
	return lease, true, nil
}

func portBaseCapacity() (uint64, error) {
	if basePortStep == 0 || portLeaseSpan == 0 {
		return 0, moerr.NewInvalidStateNoCtxf(
			"invalid embedded cluster port lease: step=%d span=%d",
			basePortStep,
			portLeaseSpan,
		)
	}
	return (portLeaseSpan - 1) / basePortStep, nil
}

func validateInitialCNPortCapacity(cn int) error {
	if cn < 0 {
		return moerr.NewInvalidInputNoCtxf("CN count cannot be negative: %d", cn)
	}
	capacity, err := portBaseCapacity()
	if err != nil {
		return err
	}
	fixed := clusterInfrastructurePortBaseCount + tnPortBaseCount
	if capacity < fixed {
		return moerr.NewInvalidStateNoCtxf(
			"embedded cluster port lease has %d slots, fewer than %d required infrastructure slots",
			capacity,
			fixed,
		)
	}
	maxCN := (capacity - fixed) / cnPortBaseCount
	if uint64(cn) > maxCN {
		return moerr.NewInvalidInputNoCtxf(
			"CN count %d exceeds embedded cluster port lease capacity %d",
			cn,
			maxCN,
		)
	}
	return nil
}

func (c *cluster) validateAdditionalCNPortCapacityLocked(n int) error {
	if n < 0 {
		return moerr.NewInvalidInputNoCtxf("additional CN count cannot be negative: %d", n)
	}
	if c.portLease == nil {
		return moerr.NewInvalidStateNoCtx("embedded cluster has no port lease")
	}
	if basePortStep == 0 || portLeaseSpan == 0 {
		return moerr.NewInvalidStateNoCtxf(
			"invalid embedded cluster port lease: step=%d span=%d",
			basePortStep,
			portLeaseSpan,
		)
	}
	current := c.portLease.next.Load()
	base := c.portLease.base
	if current < base || current-base >= portLeaseSpan {
		return moerr.NewInvalidStateNoCtxf(
			"embedded cluster port allocator is outside its lease: base=%d current=%d span=%d",
			base,
			current,
			portLeaseSpan,
		)
	}
	remainingSlots := (portLeaseSpan - 1 - (current - base)) / basePortStep
	maxAdditionalCN := remainingSlots / cnPortBaseCount
	if uint64(n) > maxAdditionalCN {
		return moerr.NewInvalidInputNoCtxf(
			"cannot add %d CN services: embedded cluster port lease has capacity for %d",
			n,
			maxAdditionalCN,
		)
	}
	return nil
}

func (c *cluster) nextBasePort() (int, error) {
	if c.portLease == nil {
		return 0, moerr.NewInvalidStateNoCtx("embedded cluster has no port lease")
	}
	base := c.portLease.base
	for {
		current := c.portLease.next.Load()
		if current < base || basePortStep == 0 || portLeaseSpan == 0 ||
			current-base >= portLeaseSpan ||
			basePortStep >= portLeaseSpan-(current-base) {
			return 0, moerr.NewInvalidStateNoCtxf(
				"embedded cluster %d exhausted port range [%d, %d)",
				c.id,
				base,
				base+portLeaseSpan,
			)
		}
		next := current + basePortStep
		if !c.portLease.next.CompareAndSwap(current, next) {
			continue
		}
		c.portLeaseNext = next
		return int(next), nil
	}
}

func (c *cluster) ensurePortLeaseLocked() error {
	if c.portLease != nil {
		return nil
	}
	lease, locked, err := tryAcquireClusterPortLease(c.portLeaseBase)
	if err != nil {
		return err
	}
	if !locked {
		return moerr.NewInvalidStateNoCtxf(
			"embedded cluster port range [%d, %d) is in use",
			c.portLeaseBase, c.portLeaseBase+portLeaseSpan)
	}
	lease.next.Store(c.portLeaseNext)
	c.portLease = lease
	return nil
}

func (c *cluster) releasePortLeaseLocked() error {
	if c.portLease == nil {
		return nil
	}
	c.portLeaseNext = c.portLease.next.Load()
	err := c.portLease.lock.Close()
	if err == nil {
		c.portLease = nil
	}
	return err
}

func (c *cluster) releaseTestAdmissionLocked() error {
	if c.testAdmission == nil {
		return nil
	}
	if err := c.testAdmission.Release(); err != nil {
		return err
	}
	c.testAdmission = nil
	return nil
}

func genConfig(
	file string,
	cfg string,
) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(cfg); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

func getInitValue(name string) uint64 {
	if name == "" {
		panic("name cannot be empty")
	}

	fileName := filepath.Join(
		os.TempDir(),
		name,
	)

	fl := flock.New(fmt.Sprintf("%s.lock", fileName))
	if err := fl.Lock(); err != nil {
		panic(err)
	}
	defer func() {
		if err := fl.Unlock(); err != nil {
			panic(err)
		}
	}()

	exists := true
	f, err := os.Open(fileName)
	if err != nil {
		if !os.IsNotExist(err) {
			panic(err)
		}
		exists = false
	} else {
		if err := f.Close(); err != nil {
			panic(err)
		}
	}

	flag := os.O_RDWR
	if !exists {
		flag |= os.O_CREATE
	}
	file, err := os.OpenFile(fileName, flag, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	data := make([]byte, 8)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		panic(err)
	}
	value := minPort
	if n > 0 {
		value = binary.BigEndian.Uint64(data)
	}
	if value > maxPort {
		value = minPort
	}
	binary.BigEndian.PutUint64(data, value+1000)

	if _, err = file.Seek(0, 0); err != nil {
		panic(err)
	}

	_, err = file.Write(data)
	if err != nil {
		panic(err)
	}
	if err := file.Sync(); err != nil {
		panic(err)
	}

	dir, err := os.Open(filepath.Dir(fileName))
	if err != nil {
		panic(err)
	}
	if err := dir.Sync(); err != nil {
		panic(err)
	}

	return uint64(value)
}
