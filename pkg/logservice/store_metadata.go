// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"path/filepath"
	"runtime"

	"github.com/cockroachdb/errors/oserror"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap"
)

const (
	logMetadataFilename = "mo-logservice.metadata"
	defaultDirFileMode  = 0750
)

func ws(err error) error {
	return err
}

func dirExist(name string, fs vfs.FS) (result bool, err error) {
	if name == "." || name == "/" {
		return true, nil
	}
	f, err := fs.OpenDir(name)
	if err != nil && oserror.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() {
		err = firstError(err, ws(f.Close()))
	}()
	s, err := f.Stat()
	if err != nil {
		return false, ws(err)
	}
	if !s.IsDir() {
		panic("not a dir")
	}
	return true, nil
}

func mkdirAll(dir string, fs vfs.FS) error {
	exist, err := dirExist(dir, fs)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	parent := fs.PathDir(dir)
	exist, err = dirExist(parent, fs)
	if err != nil {
		return err
	}
	if !exist {
		if err := mkdirAll(parent, fs); err != nil {
			return err
		}
	}
	return mkdir(dir, fs)
}

func mkdir(dir string, fs vfs.FS) error {
	parent := fs.PathDir(dir)
	exist, err := dirExist(parent, fs)
	if err != nil {
		return err
	}
	if !exist {
		panic(fmt.Sprintf("%s doesn't exist when creating %s", parent, dir))
	}
	if err := fs.MkdirAll(dir, defaultDirFileMode); err != nil {
		return err
	}
	return syncDir(parent, fs)
}

func syncDir(dir string, fs vfs.FS) (err error) {
	if runtime.GOOS == "windows" {
		return nil
	}
	if dir == "." {
		return nil
	}
	f, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, ws(f.Close()))
	}()
	fileInfo, err := f.Stat()
	if err != nil {
		return ws(err)
	}
	if !fileInfo.IsDir() {
		panic("not a dir")
	}
	df, err := fs.OpenDir(filepath.Clean(dir))
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, ws(df.Close()))
	}()
	return ws(df.Sync())
}

func getHash(data []byte) []byte {
	h := md5.New()
	if _, err := h.Write(data); err != nil {
		panic(err)
	}
	s := h.Sum(nil)
	return s[8:]
}

func exist(name string, fs vfs.FS) (bool, error) {
	if name == "." || name == "/" {
		return true, nil
	}
	_, err := fs.Stat(name)
	if err != nil && oserror.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func createMetadataFile(dir string,
	filename string, obj Marshaller, fs vfs.FS) (err error) {
	de, err := dirExist(dir, fs)
	if err != nil {
		return err
	}
	if !de {
		if err := mkdirAll(dir, fs); err != nil {
			return err
		}
	}
	tmp := fs.PathJoin(dir, fmt.Sprintf("%s.tmp", filename))
	fp := fs.PathJoin(dir, filename)
	f, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, f.Close())
		err = firstError(err, syncDir(dir, fs))
	}()
	data := MustMarshal(obj)
	h := getHash(data)
	n, err := f.Write(h)
	if err != nil {
		return ws(err)
	}
	if n != len(h) {
		return ws(io.ErrShortWrite)
	}
	n, err = f.Write(data)
	if err != nil {
		return ws(err)
	}
	if n != len(data) {
		return ws(io.ErrShortWrite)
	}
	if err := ws(f.Sync()); err != nil {
		return err
	}
	return fs.Rename(tmp, fp)
}

func readMetadataFile(dir string,
	filename string, obj Unmarshaler, fs vfs.FS) (err error) {
	fp := fs.PathJoin(dir, filename)
	f, err := fs.Open(filepath.Clean(fp))
	if err != nil {
		return err
	}
	defer func() {
		err = firstError(err, ws(f.Close()))
	}()
	data, err := io.ReadAll(f)
	if err != nil {
		return ws(err)
	}
	if len(data) < 8 {
		panic("corrupted flag file")
	}
	h := data[:8]
	buf := data[8:]
	expectedHash := getHash(buf)
	if !bytes.Equal(h, expectedHash) {
		panic("corrupted flag file content")
	}
	MustUnmarshal(obj, buf)
	return nil
}

func hasMetadataRec(dir string,
	filename string, shardID uint64, replicaID uint64, fs vfs.FS) (has bool, err error) {
	var md metadata.LogStore
	if err := readMetadataFile(dir, filename, &md, fs); err != nil {
		return false, err
	}
	for _, rec := range md.Shards {
		if rec.ShardID == shardID && rec.ReplicaID == replicaID {
			return true, nil
		}
	}
	return false, nil
}

func (l *store) loadMetadata() error {
	fs := l.cfg.FS
	dir := l.cfg.DataDir
	fp := fs.PathJoin(dir, logMetadataFilename)
	found, err := exist(fp, fs)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	expectedUUID := l.mu.metadata.UUID
	if err := readMetadataFile(dir, logMetadataFilename, &l.mu.metadata, fs); err != nil {
		return err
	}
	if expectedUUID != l.mu.metadata.UUID {
		l.runtime.Logger().Panic("unexpected UUID",
			zap.String("on disk UUID", l.mu.metadata.UUID),
			zap.String("expect", expectedUUID))
	}
	return nil
}

func (l *store) mustSaveMetadata() {
	fs := l.cfg.FS
	dir := l.cfg.DataDir
	if err := createMetadataFile(dir, logMetadataFilename, &l.mu.metadata, fs); err != nil {
		l.runtime.Logger().Panic("failed to save metadata file", zap.Error(err))
	}
}

func (l *store) addMetadata(shardID uint64, replicaID uint64, nonVoting bool) {
	rec := metadata.LogShard{}
	rec.ShardID = shardID
	rec.ReplicaID = replicaID
	rec.NonVoting = nonVoting
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, rec := range l.mu.metadata.Shards {
		if rec.ShardID == shardID && rec.ReplicaID == replicaID {
			l.runtime.Logger().Info(fmt.Sprintf("addMetadata for shardID %d, "+
				"replicaID %d skipped, dupl shard", shardID, replicaID))
			return
		}
	}

	l.mu.metadata.Shards = append(l.mu.metadata.Shards, rec)
	l.mustSaveMetadata()
}

func (l *store) removeMetadata(shardID uint64, replicaID uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	shards := make([]metadata.LogShard, 0)
	for _, rec := range l.mu.metadata.Shards {
		if rec.ShardID != shardID || rec.ReplicaID != replicaID {
			shards = append(shards, rec)
		}
	}
	l.mu.metadata.Shards = shards
	l.mustSaveMetadata()
}

func (l *store) getReplicaID(shardID uint64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, rec := range l.mu.metadata.Shards {
		if rec.ShardID == shardID {
			return int64(rec.ReplicaID)
		}
	}
	return -1
}

func (l *store) getShards() []metadata.LogShard {
	l.mu.Lock()
	defer l.mu.Unlock()
	shards := make([]metadata.LogShard, 0, len(l.mu.metadata.Shards))
	shards = append(shards, l.mu.metadata.Shards...)
	return shards
}
