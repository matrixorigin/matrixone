// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"go.uber.org/zap"
)

// counterLogExporter is the log exporter of proxy counter set.
type counterLogExporter struct {
	counter *counterSet
}

var _ stats.LogExporter = (*counterLogExporter)(nil)

// newCounterLogExporter creates a new log exporter.
func newCounterLogExporter(counter *counterSet) stats.LogExporter {
	return &counterLogExporter{
		counter: counter,
	}
}

// Export implements the stats.LogExporter interface.
func (e *counterLogExporter) Export() []zap.Field {
	var fields []zap.Field
	fields = append(fields, zap.Int64("accepted connections",
		e.counter.connAccepted.Load()))
	fields = append(fields, zap.Int64("total connections",
		e.counter.connTotal.Load()))
	fields = append(fields, zap.Int64("client disconnect",
		e.counter.clientDisconnect.Load()))
	fields = append(fields, zap.Int64("server disconnect",
		e.counter.serverDisconnect.Load()))
	fields = append(fields, zap.Int64("refused connections",
		e.counter.connRefused.Load()))
	fields = append(fields, zap.Int64("auth failed",
		e.counter.authFailed.Load()))
	fields = append(fields, zap.Int64("connection migration success",
		e.counter.connMigrationSuccess.Load()))
	fields = append(fields, zap.Int64("connection migration requested",
		e.counter.connMigrationRequested.Load()))
	fields = append(fields, zap.Int64("connection migration cannot start",
		e.counter.connMigrationCannotStart.Load()))
	return fields
}

// counterSet contains all items that need to be tracked in proxy.
type counterSet struct {
	connAccepted             stats.Counter
	connTotal                stats.Counter
	clientDisconnect         stats.Counter
	serverDisconnect         stats.Counter
	connRefused              stats.Counter
	authFailed               stats.Counter
	connMigrationSuccess     stats.Counter
	connMigrationRequested   stats.Counter
	connMigrationCannotStart stats.Counter
}

// newCounterSet creates a new counterSet.
func newCounterSet() *counterSet {
	return &counterSet{}
}

// updateWithError updates the counterSet according to the error.
func (s *counterSet) updateWithErr(err error) {
	if err == nil {
		return
	}
	switch getErrorCode(err) {
	case codeAuthFailed:
		s.authFailed.Add(1)
	case codeClientDisconnect:
		s.clientDisconnect.Add(1)
	case codeServerDisconnect:
		s.serverDisconnect.Add(1)
	}
}
