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
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"os"
	"path/filepath"
)

type NodeReader struct {
	Handle   iface.INodeHandle
	Filename []byte
	Reader   io.Reader
}

func NewNodeReader(nh iface.INodeHandle, filename []byte, reader io.Reader) iface.Reader {
	nr := &NodeReader{
		Handle: nh,
		Reader: reader,
	}

	if nr.Reader == nil {
		nr.Filename = filename
	}
	return nr
}

func (nr *NodeReader) Load() (err error) {
	node := nr.Handle.GetBuffer().GetDataNode()
	if nr.Reader != nil {
		_, err = node.ReadFrom(nr.Reader)
		return err
	}
	filename := string(nr.Filename)
	dir := filepath.Dir(filename)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
	}
	if err != nil {
		return err
	}

	w, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	_, err = node.ReadFrom(w)
	if err != nil {
		return err
	}
	// nr.Filename = fname
	return err
}
