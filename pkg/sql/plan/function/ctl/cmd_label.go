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
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type cnLabel struct {
	uuid   string
	key    string
	values []string
}

type cnWorkState struct {
	uuid  string
	state int
}

type parser interface {
	parseLabel() cnLabel
	parseWorkState() cnWorkState
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

func (s *singleValue) parseLabel() cnLabel {
	items := s.reg.FindStringSubmatch(s.s)
	var c cnLabel
	c.uuid = items[1]
	c.key = items[2]
	c.values = []string{items[3]}
	return c
}

func (s *singleValue) parseWorkState() cnWorkState {
	items := s.reg.FindStringSubmatch(s.s)
	var c cnWorkState
	c.uuid = items[1]
	c.state, _ = strconv.Atoi(items[2])
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

func (m *multiValues) parseLabel() cnLabel {
	items := m.reg.FindStringSubmatch(m.s)
	var c cnLabel
	c.uuid = items[1]
	c.key = items[2]
	c.values = strings.Split(items[3], ",")
	return c
}

// not implemented.
func (m *multiValues) parseWorkState() cnWorkState {
	return cnWorkState{}
}

const (
	singlePattern      = `^([a-zA-Z0-9\-_]+):([a-zA-Z0-9_]+):([a-zA-Z0-9_]+)$`
	multiplePattern    = `^([a-zA-Z0-9\-_]+):([a-zA-Z0-9_]+):\[([a-zA-Z0-9_]+(,[a-zA-Z0-9_]+)*)\]$`
	singlePatternState = `^([a-zA-Z0-9\-_]+):([0-9]+)$`
)

var (
	singlePatternReg      = regexp.MustCompile(singlePattern)
	multiPatternReg       = regexp.MustCompile(multiplePattern)
	singleStatePatternReg = regexp.MustCompile(singlePatternState)
)

func identifyParser(param string) parser {
	if matched := singlePatternReg.MatchString(param); matched {
		return newSingleValue(param, singlePatternReg)
	}
	if matched := multiPatternReg.MatchString(param); matched {
		return newMultiValue(param, multiPatternReg)
	}
	return nil
}

func identifyStateParser(param string) parser {
	if matched := singleStatePatternReg.MatchString(param); matched {
		return newSingleValue(param, singleStatePatternReg)
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
	return p.parseLabel(), nil
}

func handleSetLabel(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	cluster := clusterservice.GetMOCluster()
	c, err := parseCNLabel(parameter)
	if err != nil {
		return Result{}, err
	}
	kvs := make(map[string][]string, 1)
	kvs[c.key] = c.values
	if err := cluster.DebugUpdateCNLabel(c.uuid, kvs); err != nil {
		return Result{}, err
	}
	return Result{
		Method: LabelMethod,
		Data:   "OK",
	}, nil
}

func parseCNWorkState(param string) (cnWorkState, error) {
	p := identifyStateParser(param)
	if p == nil {
		return cnWorkState{}, moerr.NewInternalErrorNoCtx("format is: cn:key:value or cn:key:[v1,v2,...]")
	}
	return p.parseWorkState(), nil
}

func handleSetWorkState(proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	cluster := clusterservice.GetMOCluster()
	c, err := parseCNWorkState(parameter)
	if err != nil {
		return Result{}, err
	}
	if err := cluster.DebugUpdateCNWorkState(c.uuid, c.state); err != nil {
		return Result{}, err
	}
	return Result{
		Method: LabelMethod,
		Data:   "OK",
	}, nil
}

func handleSyncCommit(
	proc *process.Process,
	service serviceType,
	parameter string,
	sender requestSender) (Result, error) {
	qt := proc.QueryClient
	mc := clusterservice.GetMOCluster()
	var addrs []string
	mc.GetCNService(
		clusterservice.NewSelector(),
		func(c metadata.CNService) bool {
			addrs = append(addrs, c.QueryAddress)
			return true
		})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	maxCommitTS := timestamp.Timestamp{}
	for _, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_GetCommit)
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		if maxCommitTS.Less(resp.GetCommit.CurrentCommitTS) {
			maxCommitTS = resp.GetCommit.CurrentCommitTS
		}
		qt.Release(resp)
	}

	for _, addr := range addrs {
		req := qt.NewRequest(querypb.CmdMethod_SyncCommit)
		req.SycnCommit = &querypb.SyncCommitRequest{LatestCommitTS: maxCommitTS}
		resp, err := qt.SendMessage(ctx, addr, req)
		if err != nil {
			return Result{}, err
		}
		qt.Release(resp)
	}

	return Result{
		Method: SyncCommitMethod,
		Data: fmt.Sprintf("sync %d cn services's commit ts to %s",
			len(addrs),
			maxCommitTS.DebugString()),
	}, nil
}
