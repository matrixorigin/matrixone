// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
)

var (
	defaultLockListenAddress      = "127.0.0.1:6003"
	defaultMaxLockRowCount        = 1000000
	defaultMaxFixedSliceSize      = 1 << 20 * 10 // 10mb
	defaultKeepRemoteLockDuration = time.Second
	defaultKeepBindTimeout        = time.Second * 10

	MaxLockCount = float64(10_000_000)
)

// Config lock service config
type Config struct {
	// ServiceID service id
	ServiceID string `toml:"-"`
	// RPC rpc config
	RPC morpc.Config `toml:"-"`
	// TxnIterFunc used to iterate all active transactions in current cn
	TxnIterFunc func(func([]byte) bool) `toml:"-"`
	// disconnectAfterRead for testing
	disconnectAfterRead int `toml:"-"`

	// ListenAddress lock service listen address for receiving lock requests
	ListenAddress string `toml:"listen-address"`
	// ServiceAddress service address for communication, if this address is not set, use
	// ListenAddress as the communication address.
	ServiceAddress string `toml:"service-address"`
	// MaxFixedSliceSize lockservice uses fixedSlice to store all lock information, a pool
	// of fixedSlice will be built internally, there are various specifications of fixexSlice,
	// this value sets how big the maximum specification of FixedSlice is.
	MaxFixedSliceSize toml.ByteSize `toml:"max-fixed-slice-size"`
	// KeepBindDuration Maintain the period of the locktable bound to the current service
	KeepBindDuration toml.Duration `toml:"keep-bind-duration"`
	// KeepRemoteLockDuration how often to send a heartbeat to maintain a lock on a remote
	// locktable.
	KeepRemoteLockDuration toml.Duration `toml:"keep-remote-lock-duration"`
	// RemoteLockTimeout how long does it take to receive a heartbeat that maintains the
	// remote lock before releasing the lock
	RemoteLockTimeout toml.Duration `toml:"remote-lock-timeout"`
	// MaxLockRowCount each time a lock is added, some LockRow is stored in the lockservice, if
	// too many LockRows are put in each time, it will cause too much memory overhead, this value
	// limits the maximum count of LocRow put into the LockService each time, beyond this value it
	// will be converted into a Range of locks
	MaxLockRowCount toml.ByteSize `toml:"max-row-lock-count"`
	// KeepBindTimeout when a locktable is assigned to a lockservice, the lockservice will
	// continuously hold the bind, and if no hold request is received after the configured time,
	// then all bindings for the service will fail.
	KeepBindTimeout toml.Duration `toml:"keep-bind-timeout"`
	// EnableRemoteLocalProxy enable remote local proxy. The proxy used to reduce remote shared
	// lock and unlock request.
	EnableRemoteLocalProxy bool `toml:"enable-remote-local-proxy"`
}

// Validate validate
func (c *Config) Validate() {
	if c.ServiceID == "" {
		panic("missing service id")
	}
	if c.ListenAddress == "" {
		c.ListenAddress = defaultLockListenAddress
	}
	if c.ServiceAddress == "" {
		c.ServiceAddress = c.ListenAddress
	}
	if c.MaxLockRowCount == 0 {
		c.MaxLockRowCount = toml.ByteSize(defaultMaxLockRowCount)
	}
	if c.MaxFixedSliceSize == 0 {
		c.MaxFixedSliceSize = toml.ByteSize(defaultMaxFixedSliceSize)
		MaxLockCount = 0.9 * float64(c.MaxFixedSliceSize)
	}
	if c.KeepBindDuration.Duration == 0 {
		c.KeepBindDuration.Duration = time.Second
	}
	if c.KeepRemoteLockDuration.Duration == 0 {
		c.KeepRemoteLockDuration.Duration = defaultKeepRemoteLockDuration
	}
	if c.RemoteLockTimeout.Duration == 0 {
		c.RemoteLockTimeout.Duration = c.KeepRemoteLockDuration.Duration * 10
	}
	if c.KeepBindTimeout.Duration == 0 {
		c.KeepBindTimeout.Duration = defaultKeepBindTimeout
	}
	c.RPC.Adjust()
}
