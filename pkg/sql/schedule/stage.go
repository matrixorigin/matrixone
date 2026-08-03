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
	ReasonStageSingleQueryWorker  = "single-query-worker"
	ReasonStageQueryWorkers       = "query-workers"
	ReasonStageNoQueryWorkers     = "no-query-workers"
	ReasonStageCurrentCoordinator = "current-cn-coordinator"
)

type StageRequest struct {
	QueryWorkers Workers
	CurrentCN    Worker
}

type StageDecision struct {
	Worker Worker
	Reason string
}

type StageWorkerSetDecision struct {
	Workers Workers
	Reason  string
}

func DecideSingleWorkerStagePlacement(req StageRequest) StageDecision {
	if len(req.QueryWorkers) == 1 {
		return StageDecision{
			Worker: req.QueryWorkers[0],
			Reason: ReasonStageSingleQueryWorker,
		}
	}
	return StageDecision{
		Worker: req.CurrentCN,
		Reason: ReasonStageCurrentCoordinator,
	}
}

func DecideQueryWorkerStagePlacement(req StageRequest) StageWorkerSetDecision {
	if len(req.QueryWorkers) == 0 {
		return StageWorkerSetDecision{
			Reason: ReasonStageNoQueryWorkers,
		}
	}
	return StageWorkerSetDecision{
		Workers: cloneWorkers(req.QueryWorkers),
		Reason:  ReasonStageQueryWorkers,
	}
}
