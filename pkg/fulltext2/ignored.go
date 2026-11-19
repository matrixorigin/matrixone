// Copyright 2026 Matrix Origin
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

package fulltext2

import (
	"context"
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// DatalinkError returns err, the failure to read the text of the datalink url, as the reason
// the index cannot store the row when reading it again cannot succeed: a missing or disabled
// stage, a missing file, an unsupported or invalid url, or text that cannot be extracted. An
// error that may pass, such as a timeout or a lost connection, and a source the index does not
// admit are returned unchanged.
func DatalinkError(url string, err error) error {
	if err == nil || !datalinkErrorPermanent(err) {
		return err
	}
	return moerr.NewIndexDatalinkNoCtx(url + ": " + err.Error())
}

func datalinkErrorPermanent(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || fileservice.IsRetryableStartupError(err) {
		return false
	}
	code, ok := moerr.GetMoErrCode(err)
	if !ok {
		// a url or document parser error
		return true
	}
	switch code {
	case moerr.ErrBadConfig, moerr.ErrFileNotFound, moerr.ErrNYI,
		moerr.ErrInvalidInput, moerr.ErrInvalidArg, moerr.ErrInternal:
		return true
	}
	// a source the index does not admit, such as a local file for an asynchronous index, is a
	// setting of the table, not a value of the row
	return false
}

// JSONError returns err, the failure to parse the text of a row as JSON, as the reason the index
// cannot store the row.
func JSONError(err error) error {
	if err == nil {
		return nil
	}
	return moerr.NewIndexInvalidJSONNoCtx(err.Error())
}
