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
	"encoding/json"
	"net"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
)

// LabelHash defines hash value, which is hashed from labelInfo.
type LabelHash string

// labelInfo contains tenant and labels.
// NB: It is the request from client, but not the information of server.
//
// All fields in labelInfo should start with capital letter
// to get correct hash result.
type labelInfo struct {
	Tenant Tenant
	Labels map[string]string
}

// clientInfo contains the information of client, e.g. Tenant of the client
type clientInfo struct {
	// labelInfo contains tenant and labels of the client
	labelInfo
	// username of the client, unique under each tenant
	username string
	// originIP that client used to communicate with server
	originIP net.IP
}

// sessionVarName is the session variable name which defines the label info.
var sesssionVarName = "cn_label"

// reservedLabels are the labels not allowed in user labels.
// Ref: https://dev.mysql.com/doc/refman/8.0/en/performance-schema-connection-attribute-tables.html
var reservedLabels = map[string]struct{}{
	"os_user":      {},
	"os_sudouser":  {},
	"program_name": {},
}

// newLabelInfo creates labelInfo.
func newLabelInfo(tenant Tenant, labels map[string]string) labelInfo {
	// Filter out reserved connection attributes.
	for k := range labels {
		if _, ok := reservedLabels[k]; ok || strings.HasPrefix(k, "_") {
			delete(labels, k)
		}
	}
	return labelInfo{
		Tenant: tenant,
		Labels: labels,
	}
}

// allLabels returns all labels including tenant.
func (l *labelInfo) allLabels() map[string]string {
	all := l.commonLabels()
	if l.Tenant != "" {
		all[tenantLabelKey] = string(l.Tenant)
	}
	return all
}

// commonLabels returns all labels except tenant.
func (l *labelInfo) commonLabels() map[string]string {
	common := make(map[string]string)
	for k, v := range l.Labels {
		if k != "" && v != "" {
			common[k] = v
		}
	}
	return common
}

// tenantLabel returns just tenant label.
func (l *labelInfo) tenantLabel() map[string]string {
	t := make(map[string]string)
	if l.Tenant != "" {
		t[tenantLabelKey] = string(l.Tenant)
	}
	return t
}

// isSuperTenant returns true if the tenant is sys or empty.
func (l *labelInfo) isSuperTenant() bool {
	if l.Tenant == "" || l.Tenant == "sys" {
		return true
	}
	return false
}

// genSetVarStmt returns a statement of set session variable.
func (l *labelInfo) genSetVarStmt() string {
	var builder strings.Builder
	builder.WriteString("SET SESSION ")
	builder.WriteString(sesssionVarName)
	builder.WriteString("='")
	count := len(l.allLabels())
	var i int
	for k, v := range l.allLabels() {
		i++
		builder.WriteString(k)
		builder.WriteString("=")
		builder.WriteString(v)
		if i != count {
			builder.WriteString(",")
		}
	}
	builder.WriteString("'")
	return builder.String()
}

// genSelector generates the label selector according to labels in labelInfo.
func (l *labelInfo) genSelector() clusterservice.Selector {
	return clusterservice.NewSelector().SelectByLabel(
		l.allLabels(), clusterservice.EQ,
	)
}

// getHash calculate the hash value of the labelInfo.
func (l *labelInfo) getHash() (LabelHash, error) {
	targetBytes, err := json.Marshal(*l)
	if err != nil {
		return "", err
	}
	var targetMap map[string]any
	if err = json.Unmarshal(targetBytes, &targetMap); err != nil {
		return "", err
	}
	s := sortMap(targetMap)
	s = sortSimpleMap(s)
	return LabelHash(rawHash(s)), nil
}
