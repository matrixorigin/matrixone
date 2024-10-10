// Copyright 2021 - 2024 Matrix Origin
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

//go:build linux
// +build linux

package logservice

import (
	"github.com/cakturk/go-netstat/netstat"
)

func listAllPorts() map[uint16]struct{} {
	tabs, err := netstat.TCPSocks(func(s *netstat.SockTabEntry) bool {
		return true
	})
	if err != nil {
		return nil
	}
	ret := map[uint16]struct{}{}
	for _, e := range tabs {
		ret[e.LocalAddr.Port] = struct{}{}
		ret[e.RemoteAddr.Port] = struct{}{}
	}
	return ret
}
