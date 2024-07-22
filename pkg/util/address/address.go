// Copyright 2023 Matrix Origin
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

package address

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	defaultListenAddressHost = "0.0.0.0"
	defaultReservedSlots     = 20
)

// Address is used to describe the address of a MO running service, divided into 2 addresses,
// ListenAddress and ServiceAddress.
//
// ListenAddress is used to indicate the address of the service listener, used to accept external
// connections.
//
// ServiceAddress is used to register to the HAKeeper address, other nodes can get this address to
// connect to this MO's service
//
// TODO(fagongzi): refactor all address configurations in MO.
type Address struct {
	// ListenAddress listen address
	ListenAddress string `toml:"listen-address"`
	// ServiceAddress service address
	ServiceAddress string `toml:"service-addresss"`
}

// Adjust adjust address according to the rules:
// 1. If ListenAddress is not set, then use defaultListenAddress
// 2. if ServiceAddress is not set, then use ListenAddress
// 3. if machineHost is set, then replace the host of ServiceAddress with machineHost
func (addr *Address) Adjust(
	machineHost string,
	defaultListenAddress string) {
	if addr.ListenAddress == "" {
		addr.ListenAddress = defaultListenAddress
	}
	if addr.ServiceAddress == "" {
		addr.ServiceAddress = addr.ListenAddress
	}
	if machineHost != "" {
		addr.ServiceAddress = replaceHost(addr.ServiceAddress, machineHost)
	}
}

func replaceHost(
	address string,
	newHost string) string {
	oldHost := address[:strings.Index(address, ":")]
	if oldHost == "" {
		panic("address's host not found: " + address)
	}
	return strings.Replace(address, oldHost, newHost, 1)
}

// AddressManager manages all service names and ports. It uses unified
// listen address and service address. The port of each service is generated
// by port base and the port slot.
type AddressManager interface {
	// Register registers a service by its name and port slot.
	Register(portSlot int) int
	// ListenAddress returns the service address of the service.
	ListenAddress(slot int) string
	// ServiceAddress returns the service address of the service.
	ServiceAddress(slot int) string
}

type addressManager struct {
	portBase      int
	reservedSlots int
	address       Address
	mu            struct {
		sync.Mutex
		portAdvanced int
		services     map[int]int // slot => port number
	}
}

func NewAddressManager(serviceAddress string, portBase int) AddressManager {
	am := &addressManager{
		address: Address{
			ListenAddress:  defaultListenAddressHost,
			ServiceAddress: serviceAddress,
		},
		portBase:      portBase,
		reservedSlots: defaultReservedSlots,
	}
	am.mu.portAdvanced = 0
	am.mu.services = make(map[int]int)
	return am
}

// Register implements the AddressManager interface.
func (m *addressManager) Register(portSlot int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.portBase == 0 {
		return 0
	}
	if _, ok := m.mu.services[portSlot]; ok {
		return m.mu.services[portSlot]
	}
	m.mu.services[portSlot] = m.portAdvanceLocked()
	return m.mu.services[portSlot]
}

// ListenAddress implements the AddressManager interface.
func (m *addressManager) ListenAddress(slot int) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.mu.services[slot]
	if !ok {
		panic(fmt.Sprintf("slot %d has not been registered yet", slot))
	}
	return fmt.Sprintf("%s:%d", m.address.ListenAddress, p)
}

// ServiceAddress implements the AddressManager interface.
func (m *addressManager) ServiceAddress(slot int) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.mu.services[slot]
	if !ok {
		panic(fmt.Sprintf("slot %d has not been registered yet", slot))
	}
	return fmt.Sprintf("%s:%d", m.address.ServiceAddress, p)
}

// portAdvanceLocked advances the port and return the first available one.
func (m *addressManager) portAdvanceLocked() int {
	var port int
	for {
		port = m.portBase + m.mu.portAdvanced
		if localPortCheck(port) {
			break
		}
		m.mu.portAdvanced++
		if m.mu.portAdvanced > m.reservedSlots {
			panic(fmt.Sprintf("no ports are available between %d and %d",
				m.portBase, m.portBase+m.reservedSlots))
		}
	}
	m.mu.portAdvanced++
	return port
}

// localPortCheck checks if the local port is available to use. If the port is not used, return
// true; otherwise, return false.
func localPortCheck(port int) bool {
	return !RemoteAddressAvail(fmt.Sprintf(":%d", port), 0)
}

// RemoteAddressAvail checks if remote address can be connected.
func RemoteAddressAvail(address string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	} else {
		if conn != nil {
			_ = conn.Close()
			return true
		} else {
			return false
		}
	}
}
