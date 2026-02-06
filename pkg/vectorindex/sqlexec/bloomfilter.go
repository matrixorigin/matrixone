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

package sqlexec

import (
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

// waitBloomFilterForTableFunction blocks until it receives a bloomfilter runtime filter
// that matches tf.RuntimeFilterSpecs (if any). It is used when ivf_search acts as probe
// side in a join and build side produces a bloom runtime filter.
// We don't deserialize BloomFilter here, only keep raw bytes, which can be passed to SQL executor / table scan later.
func WaitBloomFilter(sqlproc *SqlProcess) ([]byte, error) {
	if sqlproc.Proc == nil {
		return nil, nil
	}

	if len(sqlproc.RuntimeFilterSpecs) == 0 {
		return nil, nil
	}
	spec := sqlproc.RuntimeFilterSpecs[0]
	if !spec.UseBloomFilter {
		return nil, nil
	}

	msgReceiver := message.NewMessageReceiver(
		[]int32{spec.Tag},
		message.AddrBroadCastOnCurrentCN(),
		sqlproc.Proc.GetMessageBoard(),
	)
	msgs, ctxDone, err := msgReceiver.ReceiveMessage(true, sqlproc.GetContext())
	if err != nil || ctxDone {
		return nil, err
	}

	for i := range msgs {
		m, ok := msgs[i].(message.RuntimeFilterMessage)
		if !ok {
			continue
		}
		if m.Typ != message.RuntimeFilter_BLOOMFILTER {
			continue
		}

		// runtime bloomfilter uses common/bloomfilter encoding; pass through bytes directly here
		return m.Data, nil
	}

	return nil, nil
}
