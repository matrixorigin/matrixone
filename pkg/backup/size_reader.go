// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// backupSizeReader withholds the final byte until EOF is verified. Otherwise a
// downstream LimitReader could accept a truncated object without another Read.
// The caller owns the underlying stream and closes it on every terminal path.
type backupSizeReader struct {
	reader    io.Reader
	remaining int64
}

func (r *backupSizeReader) checkEOF() error {
	var extra [1]byte
	n, err := io.ReadFull(r.reader, extra[:])
	if n != 0 {
		return moerr.NewInternalErrorNoCtx("backup source exceeds its stat size")
	}
	if err != io.EOF {
		return err
	}
	return nil
}

func (r *backupSizeReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if r.remaining == 1 {
		var last [1]byte
		n, err := r.reader.Read(last[:])
		if n == 0 {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		if err != nil && err != io.EOF {
			return 0, err
		}
		if err != io.EOF {
			if err := r.checkEOF(); err != nil {
				return 0, err
			}
		}
		p[0] = last[0]
		r.remaining = 0
		return 1, nil
	}
	if int64(len(p)) >= r.remaining {
		p = p[:r.remaining-1]
	}
	n, err := r.reader.Read(p)
	r.remaining -= int64(n)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}
