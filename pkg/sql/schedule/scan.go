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
	ReasonScanNoWorkers       = "no-workers"
	ReasonScanSingleWorker    = "single-worker"
	ReasonScanMissingStats    = "missing-stats"
	ReasonScanForceOneCN      = "force-one-cn"
	ReasonScanForceSingle     = "force-single"
	ReasonScanSmallBlocks     = "small-blocks"
	ReasonScanMultiCN         = "multi-cn-scan"
	ReasonScanQueryLocalExec  = "query-local-exec-type"
	ReasonScanQueryFallbackCN = "query-no-candidate-cn"
)

type ScanRequest struct {
	QueryWorkers         Workers
	CurrentCN            Worker
	QueryPlacementReason string
	Stats                *ScanStats
	ForceSingle          bool
	ForceMultiCN         bool
	OneCNBlockThreshold  int32
}

type ScanStats struct {
	BlockNum   int32
	Dop        int32
	ForceOneCN bool
}

type ScanDecision struct {
	Workers   Workers
	LocalOnly bool
	Reason    string
}

func DecideScanPlacement(req ScanRequest) ScanDecision {
	if reason, ok := inheritedLocalScanReason(req.QueryPlacementReason); ok {
		return localScanDecision(reason, pickLocalScanWorkers(req.QueryWorkers, req.CurrentCN))
	}
	if len(req.QueryWorkers) == 1 {
		return localScanDecision(ReasonScanSingleWorker, pickLocalScanWorkers(req.QueryWorkers, req.CurrentCN))
	}
	if req.Stats == nil {
		return localScanDecision(ReasonScanMissingStats, pickLocalScanWorkers(req.QueryWorkers, req.CurrentCN))
	}
	if req.Stats.ForceOneCN {
		return localScanDecision(ReasonScanForceOneCN, pickLocalScanWorkers(req.QueryWorkers, req.CurrentCN))
	}
	if req.ForceSingle {
		return localScanDecision(ReasonScanForceSingle, pickLocalScanWorkers(req.QueryWorkers, req.CurrentCN))
	}
	if !req.ForceMultiCN && req.Stats.BlockNum <= req.OneCNBlockThreshold {
		return localScanDecision(ReasonScanSmallBlocks, pickLocalScanWorkers(req.QueryWorkers, req.CurrentCN))
	}
	if len(req.QueryWorkers) == 0 {
		return localScanDecision(ReasonScanNoWorkers, nil)
	}
	return ScanDecision{
		Workers: cloneWorkers(req.QueryWorkers),
		Reason:  ReasonScanMultiCN,
	}
}

func localScanDecision(reason string, workers Workers) ScanDecision {
	return ScanDecision{
		Workers:   workers,
		LocalOnly: true,
		Reason:    reason,
	}
}

func inheritedLocalScanReason(reason string) (string, bool) {
	switch reason {
	case ReasonLocalExecType:
		return ReasonScanQueryLocalExec, true
	case ReasonNoCandidateCN:
		return ReasonScanQueryFallbackCN, true
	default:
		return "", false
	}
}

func pickLocalScanWorkers(workers Workers, current Worker) Workers {
	if len(workers) == 0 {
		return nil
	}
	if hasWorkerIdentity(current) {
		for _, worker := range workers {
			if sameWorker(worker, current) {
				return cloneWorkers(Workers{worker})
			}
		}
	}
	return cloneWorkers(workers[:1])
}
