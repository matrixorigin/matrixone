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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collationkey"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestValidateUniqueKeyCodecAdmission(t *testing.T) {
	ctx := context.Background()

	// Missing metadata is the legacy relation contract and must retain the
	// existing DML route.
	require.NoError(t, validateUniqueKeyCodecAdmission(ctx, &planpb.TableDef{}))

	legacy := &planpb.TableDef{UniqueKeyCodecVersion: &planpb.UniqueKeyCodecVersion{Value: uint32(collationkey.LegacyVersion)}}
	require.NoError(t, validateUniqueKeyCodecAdmission(ctx, legacy))

	bytewise := &planpb.TableDef{UniqueKeyCodecVersion: &planpb.UniqueKeyCodecVersion{Value: uint32(collationkey.BytewiseVersion)}}
	require.NoError(t, validateUniqueKeyCodecAdmission(ctx, bytewise))

	metadata := collationkey.NewCollationAwareMetadata()
	v2 := &planpb.TableDef{UniqueKeyCodecVersion: &planpb.UniqueKeyCodecVersion{
		Value:              metadata.Version,
		RegistryVersion:    metadata.RegistryVersion,
		RegistryDigest:     metadata.RegistryDigest,
		MaxEncodedKeyBytes: metadata.MaxEncodedKeyBytes,
	}}
	err := validateUniqueKeyCodecAdmission(ctx, v2)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnsupportedDML))
	require.ErrorContains(t, err, "collation-aware unique-key writes are not enabled")
}

func TestValidateUniqueKeyCodecAdmissionFailsClosed(t *testing.T) {
	metadata := collationkey.NewCollationAwareMetadata()
	metadata.RegistryDigest[0] ^= 0xff
	bad := &planpb.TableDef{UniqueKeyCodecVersion: &planpb.UniqueKeyCodecVersion{
		Value:              metadata.Version,
		RegistryVersion:    metadata.RegistryVersion,
		RegistryDigest:     metadata.RegistryDigest,
		MaxEncodedKeyBytes: metadata.MaxEncodedKeyBytes,
	}}
	err := validateUniqueKeyCodecAdmission(context.Background(), bad)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	require.ErrorContains(t, err, "invalid unique-key codec metadata")
}
