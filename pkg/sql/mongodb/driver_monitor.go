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

package mongodb

import (
	"context"
	"strings"
	"sync"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// driverMonitorState retains endpoint identity only inside one driver client
// so command events can be reduced to a bounded server-role label. Endpoint,
// namespace, command document, reply, and errors are never logged or exported.
type driverMonitorState struct {
	mu    sync.RWMutex
	roles map[string]string
}

func newDriverMonitors() (*event.CommandMonitor, *event.PoolMonitor, *event.ServerMonitor) {
	state := &driverMonitorState{roles: make(map[string]string)}
	commandMonitor := &event.CommandMonitor{
		Started: func(_ context.Context, command *event.CommandStartedEvent) {
			name := boundedCommandLabel(command.CommandName)
			metric.MongoDBDriverCommandCounter.WithLabelValues(name, "started").Inc()
			metric.MongoDBSelectedServerRoleCounter.WithLabelValues(state.roleForConnection(command.ConnectionID)).Inc()
		},
		Succeeded: func(_ context.Context, command *event.CommandSucceededEvent) {
			name := boundedCommandLabel(command.CommandName)
			metric.MongoDBDriverCommandCounter.WithLabelValues(name, "succeeded").Inc()
			metric.MongoDBDriverCommandDurationHistogram.WithLabelValues(name, "succeeded").Observe(command.Duration.Seconds())
		},
		Failed: func(_ context.Context, command *event.CommandFailedEvent) {
			name := boundedCommandLabel(command.CommandName)
			metric.MongoDBDriverCommandCounter.WithLabelValues(name, "failed").Inc()
			metric.MongoDBDriverCommandDurationHistogram.WithLabelValues(name, "failed").Observe(command.Duration.Seconds())
			if name == "find" && mongo.IsNetworkError(command.Failure) {
				metric.MongoDBRetryableFindCounter.Inc()
			}
		},
	}
	poolMonitor := &event.PoolMonitor{Event: func(poolEvent *event.PoolEvent) {
		typeLabel := boundedPoolEventLabel(poolEvent.Type)
		metric.MongoDBPoolEventCounter.WithLabelValues(typeLabel, boundedPoolReasonLabel(poolEvent.Reason)).Inc()
		if poolEvent.Duration > 0 {
			metric.MongoDBPoolEventDurationHistogram.WithLabelValues(typeLabel).Observe(poolEvent.Duration.Seconds())
		}
		switch poolEvent.Type {
		case event.ConnectionCheckedOut:
			metric.MongoDBPoolCheckedOutGauge.Inc()
		case event.ConnectionCheckedIn:
			metric.MongoDBPoolCheckedOutGauge.Dec()
		}
	}}
	serverMonitor := &event.ServerMonitor{
		ServerDescriptionChanged: func(serverEvent *event.ServerDescriptionChangedEvent) {
			role := boundedServerRole(serverEvent.NewDescription.Kind)
			state.mu.Lock()
			state.roles[serverEvent.Address.String()] = role
			state.mu.Unlock()
		},
		ServerClosed: func(serverEvent *event.ServerClosedEvent) {
			state.mu.Lock()
			delete(state.roles, serverEvent.Address.String())
			state.mu.Unlock()
		},
		ServerHeartbeatSucceeded: func(serverEvent *event.ServerHeartbeatSucceededEvent) {
			metric.MongoDBServerHeartbeatDurationHistogram.WithLabelValues("succeeded").Observe(serverEvent.Duration.Seconds())
		},
		ServerHeartbeatFailed: func(serverEvent *event.ServerHeartbeatFailedEvent) {
			metric.MongoDBServerHeartbeatDurationHistogram.WithLabelValues("failed").Observe(serverEvent.Duration.Seconds())
		},
	}
	return commandMonitor, poolMonitor, serverMonitor
}

func (state *driverMonitorState) roleForConnection(connectionID string) string {
	address := connectionID
	if suffix := strings.LastIndex(address, "[-"); suffix >= 0 {
		address = address[:suffix]
	}
	state.mu.RLock()
	role := state.roles[address]
	state.mu.RUnlock()
	if role == "" {
		return "unknown"
	}
	return role
}

func boundedCommandLabel(name string) string {
	switch strings.ToLower(name) {
	case "find":
		return "find"
	case "getmore":
		return "get_more"
	case "killcursors":
		return "kill_cursors"
	default:
		return "other"
	}
}

func boundedPoolEventLabel(name string) string {
	switch name {
	case event.ConnectionPoolCreated:
		return "pool_created"
	case event.ConnectionPoolReady:
		return "pool_ready"
	case event.ConnectionPoolCleared:
		return "pool_cleared"
	case event.ConnectionPoolClosed:
		return "pool_closed"
	case event.ConnectionCreated:
		return "connection_created"
	case event.ConnectionReady:
		return "connection_ready"
	case event.ConnectionClosed:
		return "connection_closed"
	case event.ConnectionCheckOutStarted:
		return "checkout_started"
	case event.ConnectionCheckOutFailed:
		return "checkout_failed"
	case event.ConnectionCheckedOut:
		return "checked_out"
	case event.ConnectionCheckedIn:
		return "checked_in"
	default:
		return "other"
	}
}

func boundedPoolReasonLabel(reason string) string {
	switch reason {
	case "":
		return "none"
	case event.ReasonIdle:
		return "idle"
	case event.ReasonPoolClosed:
		return "pool_closed"
	case event.ReasonStale:
		return "stale"
	case event.ReasonConnectionErrored:
		return "connection_error"
	case event.ReasonTimedOut:
		return "timeout"
	case event.ReasonError:
		return "error"
	default:
		return "other"
	}
}

func boundedServerRole(kind string) string {
	switch strings.ToLower(kind) {
	case "rsprimary":
		return "primary"
	case "rssecondary":
		return "secondary"
	case "standalone":
		return "standalone"
	case "mongos":
		return "mongos"
	case "loadbalancer":
		return "load_balancer"
	case "unknown", "":
		return "unknown"
	default:
		return "other"
	}
}
