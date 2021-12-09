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

package event

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type loggingListener struct{}

func NewLoggingListener() *loggingListener {
	return new(loggingListener)
}

func (l *loggingListener) OnPreSplit(event *SplitEvent) error {
	return nil
}

func (l *loggingListener) OnPostSplit(res error, event *SplitEvent) error {
	var state string
	if res != nil {
		state = res.Error()
	} else {
		state = "Done"
	}
	logutil.Infof("%s | %s", event.String(), state)
	return nil
}

func (l *loggingListener) OnBackgroundError(err error) error {
	logutil.Warn(err.Error())
	return nil
}
