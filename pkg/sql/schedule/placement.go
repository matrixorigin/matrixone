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

const (
	ReasonLocalExecType   = "local-exec-type"
	ReasonMultiCN         = "multi-cn"
	ReasonNoCandidateCN   = "no-candidate-cn"
	ReasonRequiredLocalCN = "required-local-cn"
)

type QueryExecKind uint8

const (
	QueryExecTP QueryExecKind = iota
	QueryExecAPOneCN
	QueryExecAPMultiCN
)

type QueryRequest struct {
	ExecKind     QueryExecKind
	LocalWorker  Worker
	Candidates   Workers
	RequireLocal bool
}

type QueryDecision struct {
	ExecKind QueryExecKind
	Workers  Workers
	Reason   string
}

func DecideQueryPlacement(req QueryRequest) QueryDecision {
	if req.ExecKind == QueryExecTP || req.ExecKind == QueryExecAPOneCN {
		return QueryDecision{
			ExecKind: req.ExecKind,
			Workers:  Workers{req.LocalWorker},
			Reason:   ReasonLocalExecType,
		}
	}

	workers := cloneWorkers(req.Candidates)
	reason := ReasonMultiCN
	if len(workers) == 0 {
		workers = ensureLocalWorker(workers, req.LocalWorker)
		reason = ReasonNoCandidateCN
	} else if req.RequireLocal {
		workers = ensureLocalWorker(workers, req.LocalWorker)
		reason = ReasonRequiredLocalCN
	}
	return QueryDecision{
		ExecKind: req.ExecKind,
		Workers:  workers,
		Reason:   reason,
	}
}

func ensureLocalWorker(workers Workers, local Worker) Workers {
	if local.ID == "" && local.Addr == "" {
		return workers
	}
	for _, worker := range workers {
		if sameWorker(worker, local) {
			return workers
		}
	}
	return append(workers, local)
}

func sameWorker(worker Worker, local Worker) bool {
	if worker.ID != "" && local.ID != "" && worker.ID == local.ID {
		return true
	}
	if worker.Addr != "" && local.Addr != "" && worker.Addr == local.Addr {
		return true
	}
	return false
}
