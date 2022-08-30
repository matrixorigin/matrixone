// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"fmt"
	"strings"
)

const ServiceNameSeparator = ":"

type Path struct {
	Full    string
	Service string
	File    string
}

func ParsePath(s string) (path Path, err error) {
	parts := strings.SplitN(s, ServiceNameSeparator, 2)
	switch len(parts) {
	case 1:
		// no service
		path.File = parts[0]
	case 2:
		// with service
		path.Service = parts[0]
		path.File = parts[1]
	default:
		panic("impossible")
	}
	path.Full = joinPath(path.Service, path.File)
	return
}

func ParsePathAtService(s string, serviceName string) (path Path, err error) {
	path, err = ParsePath(s)
	if err != nil {
		return
	}
	if serviceName != "" &&
		path.Service != "" &&
		!strings.EqualFold(path.Service, serviceName) {
		err = fmt.Errorf("wrong file service name, expecting %s, got %s", serviceName, path.Service)
		return
	}
	return
}

func joinPath(serviceName string, path string) string {
	buf := new(strings.Builder)
	buf.WriteString(serviceName)
	buf.WriteString(ServiceNameSeparator)
	buf.WriteString(path)
	return buf.String()
}
