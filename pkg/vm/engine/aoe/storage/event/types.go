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

import "fmt"

type SplitEvent struct {
	DB    string
	Names map[string][]string
}

func (e *SplitEvent) String() string {
	info := fmt.Sprintf("DB<\"%s\"> Splitted | {", e.DB)
	for db, tables := range e.Names {
		dbInfo := fmt.Sprintf("[\"%s\"]: ", db)
		for _, table := range tables {
			dbInfo = fmt.Sprintf("%s\"%s\" ", dbInfo, table)
		}
		dbInfo += "; "
		info = fmt.Sprintf("%s%s", info, dbInfo)
	}
	info += "}"
	return info
}

type Listener interface {
	OnDatabaseSplitted(*SplitEvent) error
	OnBackgroundError(error) error
}

type NoopListener struct{}

func (l *NoopListener) OnBackgroundError(_ error) error        { return nil }
func (l *NoopListener) OnDatabaseSplitted(_ *SplitEvent) error { return nil }
