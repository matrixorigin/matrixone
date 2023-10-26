// Copyright 2023 Matrix Origin
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

package pythonservice

import (
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
)

type service struct {
	cfg     Config
	process *os.Process
	log     io.WriteCloser
	mutex   sync.Mutex
}

func NewService(cfg Config) (PythonUdfServer, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	return &service{cfg: cfg}, nil
}

var severNo int32 = 0

func (s *service) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.process == nil {
		var err error
		var file string

		ex, err := os.Executable()
		if err != nil {
			return err
		}
		exePath := filepath.Dir(ex)

		if path.IsAbs(file) {
			file = path.Join(s.cfg.Path, "server.py")
		} else {
			file = path.Join(exePath, s.cfg.Path, "server.py")
		}
		_, err = os.Stat(file)
		if err != nil {
			return err
		}

		no := strconv.Itoa(int(atomic.AddInt32(&severNo, 1)))
		s.log, err = os.OpenFile(path.Join(exePath, "pyserver"+no+".log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				s.log.Close()
			}
		}()

		cmd := exec.Command("python", "-u", file, "--address="+s.cfg.Address)
		cmd.Stdout = s.log
		cmd.Stderr = s.log
		err = cmd.Start()
		if err != nil {
			return err
		}

		s.process = cmd.Process
	}

	return nil
}

func (s *service) Close() error {
	s.mutex.Lock()
	defer func() {
		s.mutex.Unlock()
		if s.log != nil {
			s.log.Close()
		}
	}()

	if s.process != nil {
		err := s.process.Kill()
		if err != nil {
			return err
		}
		s.process = nil
	}

	return nil
}
