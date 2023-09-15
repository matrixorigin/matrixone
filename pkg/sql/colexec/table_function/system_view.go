// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

func moConfigurationsPrepare(proc *process.Process, arg *Argument) error {
	arg.ctr.state = dataProducing
	if len(arg.Args) > 0 {
		return moerr.NewInvalidInput(proc.Ctx, "moConfigurations: no argument is required")
	}
	for i := range arg.Attrs {
		arg.Attrs[i] = strings.ToUpper(arg.Attrs[i])
	}
	return nil
}

func moConfigurationsCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	switch arg.ctr.state {
	case dataProducing:

		if proc.Hakeeper == nil {
			return false, moerr.NewInternalError(proc.Ctx, "hakeeper is nil")
		}

		//get cluster details
		details, err := proc.Hakeeper.GetClusterDetails(proc.Ctx)
		if err != nil {
			return false, err
		}

		//alloc batch
		bat := batch.NewWithSize(len(arg.Attrs))
		for i, col := range arg.Attrs {
			col = strings.ToLower(col)
			idx, ok := plan2.MoConfigColName2Index[col]
			if !ok {
				return false, moerr.NewInternalError(proc.Ctx, "bad input select columns name %v", col)
			}

			tp := plan2.MoConfigColTypes[idx]
			bat.Vecs[i] = vector.NewVec(tp)
		}
		bat.Attrs = arg.Attrs

		mp := proc.GetMPool()

		//fill batch for cn
		for _, cnStore := range details.GetCNStores() {
			if cnStore.GetConfigData() != nil {
				err = fillMapToBatch("cn", cnStore.GetUUID(), arg.Attrs, cnStore.GetConfigData().GetContent(), bat, mp)
				if err != nil {
					return false, err
				}
			}
		}

		//fill batch for tn
		for _, tnStore := range details.GetTNStores() {
			if tnStore.GetConfigData() != nil {
				err = fillMapToBatch("tn", tnStore.GetUUID(), arg.Attrs, tnStore.GetConfigData().GetContent(), bat, mp)
				if err != nil {
					return false, err
				}
			}
		}

		//fill batch for log
		for _, logStore := range details.GetLogStores() {
			if logStore.GetConfigData() != nil {
				err = fillMapToBatch("log", logStore.GetUUID(), arg.Attrs, logStore.GetConfigData().GetContent(), bat, mp)
				if err != nil {
					return false, err
				}
			}
		}

		bat.SetRowCount(bat.Vecs[0].Length())
		proc.SetInputBatch(bat)
		arg.ctr.state = dataFinished
		return false, nil

	case dataFinished:
		proc.SetInputBatch(nil)
		return true, nil
	default:
		return false, moerr.NewInternalError(proc.Ctx, "unknown state %v", arg.ctr.state)
	}
}

func fillMapToBatch(nodeType, nodeId string, attrs []string, kvs map[string]*logservicepb.ConfigItem, bat *batch.Batch, mp *mpool.MPool) error {
	var err error
	for _, value := range kvs {
		for i, col := range attrs {
			col = strings.ToLower(col)
			switch plan2.MoConfigColType(plan2.MoConfigColName2Index[col]) {
			case plan2.MoConfigColTypeNodeType:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(nodeType), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeNodeId:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(nodeId), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeName:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetName()), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeCurrentValue:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetCurrentValue()), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeDefaultValue:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetDefaultValue()), false, mp); err != nil {
					return err
				}
			case plan2.MoConfigColTypeInternal:
				if err = vector.AppendBytes(bat.Vecs[i], []byte(value.GetInternal()), false, mp); err != nil {
					return err
				}
			}
		}
	}
	return err
}
