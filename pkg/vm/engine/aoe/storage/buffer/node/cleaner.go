// Copyright 2021 Matrix Origin
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

package node

import (
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"os"
	// log "github.com/sirupsen/logrus"
)

type NodeCleaner struct {
	Filename []byte
}

func NewNodeCleaner(filename []byte) ioif.Cleaner {
	return &NodeCleaner{
		Filename: filename,
	}
}

func (nc *NodeCleaner) Clean() error {
	// log.Infof("NodeCleaner removing %s", nc.Filename)
	return os.Remove(string(nc.Filename))
}
