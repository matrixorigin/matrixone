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

package ctl

import (
	"regexp"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type cnLabel struct {
	uuid   string
	key    string
	values []string
}

type parser interface {
	parse() cnLabel
}

type singleValue struct {
	s   string
	reg *regexp.Regexp
}

func newSingleValue(s string, reg *regexp.Regexp) *singleValue {
	return &singleValue{
		s:   s,
		reg: reg,
	}
}

func (s *singleValue) parse() cnLabel {
	items := s.reg.FindStringSubmatch(s.s)
	var c cnLabel
	c.uuid = items[1]
	c.key = items[2]
	c.values = []string{items[3]}
	return c
}

type multiValues struct {
	s   string
	reg *regexp.Regexp
}

func newMultiValue(s string, reg *regexp.Regexp) *multiValues {
	return &multiValues{
		s:   s,
		reg: reg,
	}
}

func (m *multiValues) parse() cnLabel {
	items := m.reg.FindStringSubmatch(m.s)
	var c cnLabel
	c.uuid = items[1]
	c.key = items[2]
	c.values = strings.Split(items[3], ",")
	return c
}

func identifyParser(param string) parser {
	singlePattern := `^([a-zA-Z0-9\-_]+):([a-zA-Z0-9_]+):([a-zA-Z0-9_]+)$`
	multiplePattern := `^([a-zA-Z0-9\-_]+):([a-zA-Z0-9_]+):\[([a-zA-Z0-9_]+(,[a-zA-Z0-9_]+)*)\]$`
	matched, err := regexp.MatchString(singlePattern, param)
	if err != nil {
		return nil
	} else if matched {
		return newSingleValue(param, regexp.MustCompile(singlePattern))
	}
	matched, err = regexp.MatchString(multiplePattern, param)
	if err != nil {
		return nil
	} else if matched {
		return newMultiValue(param, regexp.MustCompile(multiplePattern))
	}
	return nil
}

// parseParameter parses the parameter which contains CN uuid and its label
// information. Its format can be: (1) cn:key:value (2) cn:key:[v1,v2,v3]
func parseCNLabel(param string) (cnLabel, error) {
	p := identifyParser(param)
	if p == nil {
		return cnLabel{}, moerr.NewInternalErrorNoCtx("format is: cn:key:value or cn:key:[v1,v2,...]")
	}
	return p.parse(), nil
}

func handleSetLabel(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (pb.CtlResult, error) {
	cluster := clusterservice.GetMOCluster()
	c, err := parseCNLabel(parameter)
	if err != nil {
		return pb.CtlResult{}, err
	}
	kvs := make(map[string][]string)
	kvs[c.key] = c.values
	if err := cluster.DebugUpdateCNLabel(c.uuid, kvs); err != nil {
		return pb.CtlResult{}, err
	}
	return pb.CtlResult{
		Method: pb.CmdMethod_Label.String(),
		Data:   "OK",
	}, nil
}
