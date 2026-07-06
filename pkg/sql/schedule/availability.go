// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

type SameCNFunc func(leftAddr string, rightAddr string) bool
type AvailableFunc func(addr string) bool

type AvailabilityRequest struct {
	Workers     Workers
	LocalAddr   string
	SameCN      SameCNFunc
	IsAvailable AvailableFunc
}

func FilterAvailableWorkers(req AvailabilityRequest) Workers {
	if req.IsAvailable == nil {
		return cloneWorkers(req.Workers)
	}

	workers := make(Workers, 0, len(req.Workers))
	for _, worker := range req.Workers {
		if isLocalWorker(req, worker) || req.IsAvailable(worker.Addr) {
			workers = append(workers, worker)
		}
	}
	return workers
}

func isLocalWorker(req AvailabilityRequest, worker Worker) bool {
	if req.SameCN != nil {
		return req.SameCN(req.LocalAddr, worker.Addr)
	}
	return req.LocalAddr != "" && worker.Addr != "" && worker.Addr == req.LocalAddr
}
