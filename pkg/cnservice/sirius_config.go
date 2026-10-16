// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ValidateSiriusConfigKeys rejects settings outside the current Sirius schema.
// Both service configuration loaders use the decoder's actual key metadata;
// no removed setting or compatibility spelling is retained here.
func ValidateSiriusConfigKeys(metadata toml.MetaData) error {
	for _, key := range metadata.Undecoded() {
		if len(key) >= 3 && strings.EqualFold(key[0], "cn") && strings.EqualFold(key[1], "sirius") {
			return moerr.NewBadConfigNoCtxf("unknown Sirius configuration key: %s", key.String())
		}
	}
	return nil
}
