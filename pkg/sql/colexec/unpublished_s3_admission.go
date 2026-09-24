// Copyright 2026 Matrix Origin
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

package colexec

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultUnpublishedS3TicketLimit = 65_536
	maxUnpublishedS3ObjectNameLen   = 256
)

// unpublishedS3Admission charges each live CN for object names it may still
// need to delete. Admission precedes the object Sync, so a full ledger cannot
// force an already-persisted object's cleanup owner to be discarded.
type unpublishedS3Admission struct {
	mu                 sync.Mutex
	limit              int
	names              map[string]struct{}
	highWater          int
	failedReservations uint64
	usedGauge          prometheus.Gauge
	peakGauge          prometheus.Gauge
	failedCounter      prometheus.Counter
}

// UnpublishedS3AdmissionStats is a bounded, name-free snapshot for CN
// diagnostics and tests. Names themselves must never be metric labels.
type UnpublishedS3AdmissionStats struct {
	Used               int
	Limit              int
	HighWater          int
	FailedReservations uint64
}

func (srv *Server) UnpublishedS3AdmissionStats() UnpublishedS3AdmissionStats {
	if srv == nil || srv.unpublishedS3Admission == nil {
		return UnpublishedS3AdmissionStats{}
	}
	a := srv.unpublishedS3Admission
	a.mu.Lock()
	defer a.mu.Unlock()
	return UnpublishedS3AdmissionStats{
		Used:               len(a.names),
		Limit:              a.limit,
		HighWater:          a.highWater,
		FailedReservations: a.failedReservations,
	}
}

func newUnpublishedS3Admission(limit int, serviceID ...string) *unpublishedS3Admission {
	a := &unpublishedS3Admission{limit: limit, names: make(map[string]struct{})}
	if len(serviceID) != 0 && serviceID[0] != "" {
		a.usedGauge = metricv2.UnpublishedS3TicketsGauge.WithLabelValues(serviceID[0], "used")
		a.peakGauge = metricv2.UnpublishedS3TicketsGauge.WithLabelValues(serviceID[0], "high_water")
		a.failedCounter = metricv2.UnpublishedS3AdmissionFailuresCounter.WithLabelValues(serviceID[0])
		a.usedGauge.Set(0)
		a.peakGauge.Set(0)
	}
	return a
}

func (a *unpublishedS3Admission) reserveUpload(name string) error {
	if err := validateUnpublishedS3ObjectName(name); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.names[name]; exists {
		return moerr.NewInvalidStateNoCtx("unpublished S3 object name already reserved")
	}
	if len(a.names) >= a.limit {
		a.failedReservations++
		if a.failedCounter != nil {
			a.failedCounter.Inc()
		}
		return moerr.NewResourceExhaustedf(context.Background(),
			"CN unpublished S3 cleanup capacity exhausted: used=%d limit=%d", len(a.names), a.limit)
	}
	a.names[name] = struct{}{}
	if a.usedGauge != nil {
		a.usedGauge.Set(float64(len(a.names)))
	}
	if len(a.names) > a.highWater {
		a.highWater = len(a.names)
		if a.peakGauge != nil {
			a.peakGauge.Set(float64(a.highWater))
		}
	}
	return nil
}

// reserveReceived reserves only names not already charged on this CN. A batch
// failure rolls back its new reservations; earlier owners keep their tickets.
func (a *unpublishedS3Admission) reserveReceived(names []string) ([]string, error) {
	for _, name := range names {
		if err := validateUnpublishedS3ObjectName(name); err != nil {
			return nil, err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	newNames := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, repeated := seen[name]; repeated {
			continue
		}
		seen[name] = struct{}{}
		if _, exists := a.names[name]; !exists {
			newNames = append(newNames, name)
		}
	}
	if len(newNames) > a.limit-len(a.names) {
		a.failedReservations++
		if a.failedCounter != nil {
			a.failedCounter.Inc()
		}
		return nil, moerr.NewResourceExhaustedf(context.Background(),
			"CN unpublished S3 cleanup capacity exhausted: used=%d requested=%d limit=%d",
			len(a.names), len(newNames), a.limit)
	}
	for _, name := range newNames {
		a.names[name] = struct{}{}
	}
	if a.usedGauge != nil {
		a.usedGauge.Set(float64(len(a.names)))
	}
	if len(a.names) > a.highWater {
		a.highWater = len(a.names)
		if a.peakGauge != nil {
			a.peakGauge.Set(float64(a.highWater))
		}
	}
	return newNames, nil
}

func (a *unpublishedS3Admission) release(name string) {
	if a == nil || name == "" {
		return
	}
	a.mu.Lock()
	delete(a.names, name)
	if a.usedGauge != nil {
		a.usedGauge.Set(float64(len(a.names)))
	}
	a.mu.Unlock()
}

func (a *unpublishedS3Admission) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.names)
}

func validateUnpublishedS3ObjectName(name string) error {
	if name == "" || len(name) > maxUnpublishedS3ObjectNameLen {
		return moerr.NewInvalidArgNoCtx("unpublished S3 object name", name)
	}
	return nil
}

func (srv *Server) reserveUnpublishedS3Upload(name string) error {
	if srv == nil {
		return moerr.NewInvalidStateNoCtx("missing CN unpublished S3 admission service")
	}
	return srv.unpublishedS3Admission.reserveUpload(name)
}

func (srv *Server) reserveUnpublishedS3Received(names []string) ([]string, error) {
	if srv == nil {
		return nil, moerr.NewInvalidStateNoCtx("missing CN unpublished S3 admission service")
	}
	return srv.unpublishedS3Admission.reserveReceived(names)
}
