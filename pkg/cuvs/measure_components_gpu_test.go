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

//go:build gpu

package cuvs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// MeasureComponents decides which packed bytes count against VRAM. Getting the
// classification wrong is not cosmetic: counting a host member as device
// over-states demand and over-refuses, while counting a device member as host
// UNDER-states it and admits an index that cannot load.
//
// No GPU is involved -- this is filesystem classification -- but pkg/cuvs is
// gpu-tagged as a whole, so the test lives behind the tag with its package.
func TestMeasureComponents(t *testing.T) {
	write := func(t *testing.T, dir, name string, n int) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0o600))
	}

	t.Run("splits device from host members", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "index.bin", 1000)     // device
		write(t, dir, "ids.bin", 80)         // host: host_ids
		write(t, dir, "filter_data.bin", 40) // host: INCLUDE columns
		write(t, dir, "quantizer.bin", 8)    // host
		write(t, dir, "bitset.bin", 16)      // host
		write(t, dir, "manifest.json", 6)    // host

		got, err := MeasureComponents(dir)
		require.NoError(t, err)
		require.Equal(t, int64(1000), got.Device, "only index.bin reaches the GPU")
		require.Equal(t, int64(150), got.Host)
		require.Equal(t, int64(1150), got.Total)
		require.Equal(t, got.Device+got.Host, got.Total, "the split must be lossless")
		require.Len(t, got.Files, 6)
		require.Equal(t, int64(80), got.Files["ids.bin"])
	})

	t.Run("every shard is device-resident", func(t *testing.T) {
		// Under SHARDED there is no index.bin; each rank writes its own file, and
		// all of them deserialize onto GPUs.
		dir := t.TempDir()
		write(t, dir, "shard_0.bin", 100)
		write(t, dir, "shard_1.bin", 140)
		write(t, dir, "ids.bin", 8)

		got, err := MeasureComponents(dir)
		require.NoError(t, err)
		require.Equal(t, int64(240), got.Device)
		require.Equal(t, int64(8), got.Host)
		// The per-device figure the build gate uses is the LARGEST shard, not this
		// sum and not sum/2 -- the shards are uneven because the last one absorbs
		// the remainder, so an even division under-states the biggest.
		require.Equal(t, int64(140), got.Files["shard_1.bin"])
	})

	t.Run("an unknown component counts as device", func(t *testing.T) {
		// The conservative default. A component added later and never classified
		// must over-state demand (over-refuse) rather than silently under-admit.
		dir := t.TempDir()
		write(t, dir, "index.bin", 10)
		write(t, dir, "something_new.bin", 90)

		got, err := MeasureComponents(dir)
		require.NoError(t, err)
		require.Equal(t, int64(100), got.Device)
		require.Zero(t, got.Host)
		require.False(t, IsHostResidentComponent("something_new.bin"))
	})

	t.Run("subdirectories are skipped", func(t *testing.T) {
		dir := t.TempDir()
		write(t, dir, "index.bin", 10)
		require.NoError(t, os.Mkdir(filepath.Join(dir, "nested"), 0o700))

		got, err := MeasureComponents(dir)
		require.NoError(t, err)
		require.Equal(t, int64(10), got.Total)
	})

	t.Run("empty and missing directories", func(t *testing.T) {
		got, err := MeasureComponents(t.TempDir())
		require.NoError(t, err)
		require.Zero(t, got.Total)
		require.NotNil(t, got.Files, "Files must be usable without a nil check")

		_, err = MeasureComponents(filepath.Join(t.TempDir(), "does-not-exist"))
		require.Error(t, err, "an unreadable directory must not report zero bytes")
	})
}

// MeasureTar must classify a packed archive exactly as MeasureComponents
// classifies the directory it came from. The load path depends on that: it reads
// the tar before Unpack to size the host_ids claim, and a tar-vs-directory
// disagreement would mean the build and load paths disagree about what an index
// costs.
func TestMeasureTarMatchesMeasureComponents(t *testing.T) {
	dir := t.TempDir()
	for name, n := range map[string]int{
		"index.bin": 900, "ids.bin": 88, "filter_data.bin": 40,
		"bitset.bin": 16, "manifest.json": 6, "something_new.bin": 12,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), make([]byte, n), 0o600))
	}
	fromDir, err := MeasureComponents(dir)
	require.NoError(t, err)

	tarPath := filepath.Join(t.TempDir(), "packed.tar")
	require.NoError(t, Pack(dir, tarPath))
	fromTar, err := MeasureTar(tarPath)
	require.NoError(t, err)

	require.Equal(t, fromDir.Files, fromTar.Files, "per-component sizes must agree")
	require.Equal(t, fromDir.Device, fromTar.Device, "device split must agree")
	require.Equal(t, fromDir.Host, fromTar.Host, "host split must agree")
	require.Equal(t, fromDir.Total, fromTar.Total)
	// Including the conservative default for an unclassified component.
	require.Equal(t, int64(900+12), fromTar.Device)

	t.Run("missing tar errors rather than reporting zero", func(t *testing.T) {
		_, err := MeasureTar(filepath.Join(t.TempDir(), "nope.tar"))
		require.Error(t, err, "a silent zero would size the load claim to nothing")
	})
}

// The classification has ONE owner now (helper.cpp, kHostResidentComponents) and
// Go reads it across the cgo boundary. This proves the plumbing -- a truncated or
// mis-split list would silently reclassify components, and reclassifying is how a
// component gets charged to both governors or to neither.
func TestHostResidentComponentsComeFromCpp(t *testing.T) {
	for _, name := range []string{
		"ids.bin", "filter_data.bin", "quantizer.bin", "bitset.bin", "manifest.json",
	} {
		require.True(t, IsHostResidentComponent(name), "%s must be host-resident", name)
	}
	// Device-resident by exclusion, which is the deliberate default for anything
	// the list does not name.
	for _, name := range []string{"index.bin", "shard_0.bin", "shard_7.bin", ""} {
		require.False(t, IsHostResidentComponent(name), "%q must not be host-resident", name)
	}
	// The set is exactly those five: an extra entry would mean the split picked up
	// stray whitespace or a trailing separator.
	require.Len(t, hostResidentComponents(), 5)
}
