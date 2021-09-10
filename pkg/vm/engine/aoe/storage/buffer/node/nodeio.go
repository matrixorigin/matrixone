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
	"fmt"
	"io"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	// log "github.com/sirupsen/logrus"
)

func NewNodeIOWithReader(nh iface.INodeHandle, reader io.Reader) ioif.IO {
	nio := &dio.DefaultIO{}
	nio.Reader = NewNodeReader(nh, nil, reader)
	return nio
}

func NewNodeIO(nh iface.INodeHandle, dir []byte) ioif.IO {
	nio := &dio.DefaultIO{}
	id := nh.GetID()
	filename := e.MakeFilename(string(dir), e.FTTransientNode, fmt.Sprintf("%d", id), false)
	buf := []byte(filename)
	nio.Reader = NewNodeReader(nh, buf, nil)
	nio.Writer = NewNodeWriter(nh, buf)
	nio.Cleaner = NewNodeCleaner(buf)
	return nio
}
