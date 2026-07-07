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
	ReasonScanNoWorkers    = "no-workers"
	ReasonScanSingleWorker = "single-worker"
	ReasonScanMissingStats = "missing-stats"
	ReasonScanForceOneCN   = "force-one-cn"
	ReasonScanForceSingle  = "force-single"
	ReasonScanSmallBlocks  = "small-blocks"
	ReasonScanMultiCN      = "multi-cn-scan"
)

type ScanRequest struct {
	QueryWorkers        Workers
	Stats               *ScanStats
	ForceSingle         bool
	ForceMultiCN        bool
	OneCNBlockThreshold int32
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
	if len(req.QueryWorkers) == 1 {
		return localScanDecision(ReasonScanSingleWorker)
	}
	if req.Stats == nil {
		return localScanDecision(ReasonScanMissingStats)
	}
	if req.Stats.ForceOneCN {
		return localScanDecision(ReasonScanForceOneCN)
	}
	if req.ForceSingle {
		return localScanDecision(ReasonScanForceSingle)
	}
	if !req.ForceMultiCN && req.Stats.BlockNum <= req.OneCNBlockThreshold {
		return localScanDecision(ReasonScanSmallBlocks)
	}
	if len(req.QueryWorkers) == 0 {
		return localScanDecision(ReasonScanNoWorkers)
	}
	return ScanDecision{
		Workers: cloneWorkers(req.QueryWorkers),
		Reason:  ReasonScanMultiCN,
	}
}

func localScanDecision(reason string) ScanDecision {
	return ScanDecision{
		LocalOnly: true,
		Reason:    reason,
	}
}
