// Copyright 2021 -2023 Matrix Origin
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

package status

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
)

const JsonIdent = "    "

type CNInstance struct {
	TxnClient     client.TxnClient
	LockService   lockservice.LockService
	logtailClient *disttae.PushClient
	Engine        *disttae.Engine
}

type Server struct {
	mu struct {
		sync.Mutex
		LogtailServer  *service.LogtailServer
		HAKeeperClient logservice.ClusterHAKeeperClient
		CNInstances    map[string]*CNInstance
	}
}

func NewServer() *Server {
	s := &Server{}
	s.mu.CNInstances = make(map[string]*CNInstance)
	return s
}

func (s *Server) SetLogtailServer(logtailServer *service.LogtailServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.LogtailServer = logtailServer
}

func (s *Server) SetHAKeeperClient(c logservice.ClusterHAKeeperClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.HAKeeperClient == nil {
		s.mu.HAKeeperClient = c
	}
}

func (s *Server) SetTxnClient(uuid string, c client.TxnClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.CNInstances[uuid]
	if !ok {
		s.mu.CNInstances[uuid] = &CNInstance{}
	}
	s.mu.CNInstances[uuid].TxnClient = c
}

func (s *Server) SetLockService(uuid string, l lockservice.LockService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.CNInstances[uuid]
	if !ok {
		s.mu.CNInstances[uuid] = &CNInstance{}
	}
	s.mu.CNInstances[uuid].LockService = l
}

func (s *Server) SetLogTailClient(uuid string, c *disttae.PushClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.CNInstances[uuid]
	if !ok {
		s.mu.CNInstances[uuid] = &CNInstance{}
	}
	s.mu.CNInstances[uuid].logtailClient = c
}

func (s *Server) SetEngine(uuid string, e *disttae.Engine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.mu.CNInstances[uuid]
	if !ok {
		s.mu.CNInstances[uuid] = &CNInstance{}
	}
	s.mu.CNInstances[uuid].Engine = e
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	switch path {
	case "/debug/status":
		data, err := s.Dump()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	case "/debug/status/catalog":
		s.serveCatalog(w, r)
	case "/debug/status/catalog-cache":
		s.serveCatalogCache(w, r)
	case "/debug/status/catalog-activation":
		s.serveCatalogActivation(w, r)
	case "/debug/status/partitions":
		s.servePartitions(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) Dump() ([]byte, error) {
	var status Status
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.LogtailServer != nil {
		status.LogtailServerStatus.fill(s.mu.LogtailServer)
	}
	if s.mu.HAKeeperClient != nil {
		status.HAKeeperStatus.fill(s.mu.HAKeeperClient)
	}
	status.fillCNStatus(s.mu.CNInstances)
	return json.MarshalIndent(status, "", JsonIdent)
}

func (s *Server) serveCatalog(w http.ResponseWriter, r *http.Request) {
	accountFilter, err := optionalUint32Param(r, "account")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	snapshotFilter, err := optionalTimestampParam(r, "snapshot")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cnUUID, instance, err := s.selectCNInstance(r.URL.Query().Get("cn"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if instance.Engine == nil {
		http.Error(w, "selected CN instance does not have a disttae engine registered", http.StatusBadRequest)
		return
	}

	resp := struct {
		CNUUID  string                    `json:"cn_uuid"`
		Catalog disttae.DebugCatalogState `json:"catalog"`
	}{
		CNUUID:  cnUUID,
		Catalog: instance.Engine.DebugCatalogState(accountFilter, snapshotFilter),
	}
	writeJSON(w, resp)
}

func (s *Server) serveCatalogCache(w http.ResponseWriter, r *http.Request) {
	accountID, err := requiredUint32Param(r, "account")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	snapshotFilter, err := optionalTimestampParam(r, "snapshot")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit, err := intParam(r, "limit", 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cnUUID, instance, err := s.selectCNInstance(r.URL.Query().Get("cn"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if instance.Engine == nil {
		http.Error(w, "selected CN instance does not have a disttae engine registered", http.StatusBadRequest)
		return
	}

	resp := struct {
		CNUUID       string                         `json:"cn_uuid"`
		CatalogCache disttae.DebugCatalogCacheState `json:"catalog_cache"`
	}{
		CNUUID:       cnUUID,
		CatalogCache: instance.Engine.DebugCatalogCache(accountID, snapshotFilter, r.URL.Query().Get("db"), limit),
	}
	writeJSON(w, resp)
}

func (s *Server) serveCatalogActivation(w http.ResponseWriter, r *http.Request) {
	accountFilter, err := optionalUint32Param(r, "account")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit, err := intParam(r, "limit", 50)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cnUUID, instance, err := s.selectCNInstance(r.URL.Query().Get("cn"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if instance.Engine == nil {
		http.Error(w, "selected CN instance does not have a disttae engine registered", http.StatusBadRequest)
		return
	}

	resp := struct {
		CNUUID            string                              `json:"cn_uuid"`
		CatalogActivation disttae.DebugCatalogActivationState `json:"catalog_activation"`
	}{
		CNUUID:            cnUUID,
		CatalogActivation: instance.Engine.DebugCatalogActivationHistory(accountFilter, limit),
	}
	writeJSON(w, resp)
}

func (s *Server) servePartitions(w http.ResponseWriter, r *http.Request) {
	dbFilter, err := optionalUint64Param(r, "db")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tableFilter, err := optionalUint64Param(r, "table")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit, err := intParam(r, "limit", 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cnUUID, instance, err := s.selectCNInstance(r.URL.Query().Get("cn"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if instance.Engine == nil {
		http.Error(w, "selected CN instance does not have a disttae engine registered", http.StatusBadRequest)
		return
	}

	resp := struct {
		CNUUID     string                       `json:"cn_uuid"`
		Partitions disttae.DebugPartitionsState `json:"partitions"`
	}{
		CNUUID:     cnUUID,
		Partitions: instance.Engine.DebugPartitions(dbFilter, tableFilter, limit),
	}
	writeJSON(w, resp)
}

func (s *Server) selectCNInstance(cnUUID string) (string, *CNInstance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cnUUID != "" {
		instance, ok := s.mu.CNInstances[cnUUID]
		if !ok {
			return "", nil, fmt.Errorf("cn %q not found", cnUUID)
		}
		copy := *instance
		return cnUUID, &copy, nil
	}

	switch len(s.mu.CNInstances) {
	case 0:
		return "", nil, fmt.Errorf("no CN instances registered")
	case 1:
		for uuid, instance := range s.mu.CNInstances {
			copy := *instance
			return uuid, &copy, nil
		}
	}

	uuids := make([]string, 0, len(s.mu.CNInstances))
	for uuid := range s.mu.CNInstances {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)
	return "", nil, fmt.Errorf("multiple CN instances registered, specify ?cn= (%s)", strings.Join(uuids, ", "))
}

func optionalUint32Param(r *http.Request, key string) (*uint32, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", key, err)
	}
	result := uint32(parsed)
	return &result, nil
}

func optionalUint64Param(r *http.Request, key string) (*uint64, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", key, err)
	}
	return &parsed, nil
}

func requiredUint32Param(r *http.Request, key string) (uint32, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return 0, fmt.Errorf("%s is required", key)
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	return uint32(parsed), nil
}

func optionalTimestampParam(r *http.Request, key string) (*timestamp.Timestamp, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return nil, nil
	}

	parts := strings.Split(value, ":")
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid %s: expected physical[:logical]", key)
	}

	physical, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s physical time: %w", key, err)
	}
	if physical < 0 {
		return nil, fmt.Errorf("%s physical time must be non-negative", key)
	}

	ts := &timestamp.Timestamp{PhysicalTime: physical}
	if len(parts) == 2 {
		logical, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid %s logical time: %w", key, err)
		}
		ts.LogicalTime = uint32(logical)
	}
	return ts, nil
}

func intParam(r *http.Request, key string, defaultValue int) (int, error) {
	value := r.URL.Query().Get(key)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be greater than zero", key)
	}
	return parsed, nil
}

func writeJSON(w http.ResponseWriter, value any) {
	data, err := json.MarshalIndent(value, "", JsonIdent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
