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
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"os"
	"path/filepath"
)

type NodeWriter struct {
	Handle   iface.INodeHandle
	Filename []byte
}

func NewNodeWriter(nh iface.INodeHandle, filename []byte) ioif.Writer {
	w := &NodeWriter{
		Handle:   nh,
		Filename: filename,
	}
	return w
}

func MakeNodeFileName(id uint64) string {
	return fmt.Sprintf("%d", id)
}

func (sw *NodeWriter) Flush() (err error) {
	node := sw.Handle.GetBuffer().GetDataNode()
	filename := string(sw.Filename)
	dir := filepath.Dir(filename)
	logutil.Infof(" %s | SpillNode | Flushing", sw.Filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.Create(filename)
	if err != nil {
		return err
	}
	_, err = node.WriteTo(w)
	if err != nil {
		return err
	}
	return err
}
