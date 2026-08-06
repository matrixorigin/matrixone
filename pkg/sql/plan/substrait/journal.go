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

package substrait

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// JSON base64-expands the 64 MiB manifest and 1 MiB schema by 4/3. The
// journal also carries the canonical wire and the manifest's object names.
const maxJournalRecordSize = 160 << 20

// FileServiceLeaseJournal persists leases in the shared file service. Active
// records and release markers are write-once objects, so a crash cannot expose
// a partially overwritten authority record.
type FileServiceLeaseJournal struct {
	fs     fileservice.FileService
	prefix string
}

type journalRecord struct {
	Wire                     []byte   `json:"wire"`
	Manifest                 []byte   `json:"manifest"`
	CanonicalSchema          []byte   `json:"canonical_schema"`
	AuthorizedClientSPKIHash []byte   `json:"authorized_client_spki_hash"`
	ObjectNames              []string `json:"object_names,omitempty"`
}

type journalEnvelope struct {
	Record journalRecord `json:"record"`
	SHA256 []byte        `json:"sha256"`
}

func NewFileServiceLeaseJournal(fs fileservice.FileService, prefix string) (*FileServiceLeaseJournal, error) {
	prefix = strings.Trim(path.Clean(prefix), "/")
	if fs == nil || prefix == "" || prefix == "." || strings.HasPrefix(prefix, "..") {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease journal configuration")
	}
	return &FileServiceLeaseJournal{fs: fs, prefix: prefix}, nil
}

func (j *FileServiceLeaseJournal) Store(ctx context.Context, lease *Lease) error {
	if lease == nil || lease.Read == nil || len(lease.Read.ReadRef) != 32 {
		return moerr.NewInternalErrorNoCtx("substrait: invalid lease journal record")
	}
	if _, err := j.fs.StatFile(ctx, j.releasedPath(lease.Read.ReadRef)); err == nil {
		return moerr.NewInternalErrorNoCtx("substrait: released read reference already exists")
	} else if !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return err
	}
	record := journalRecord{Wire: lease.Wire, Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema, AuthorizedClientSPKIHash: lease.AuthorizedClientSPKIHash, ObjectNames: lease.ObjectNames}
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(recordBytes)
	b, err := json.Marshal(journalEnvelope{Record: record, SHA256: digest[:]})
	if err != nil {
		return err
	}
	if len(b) > maxJournalRecordSize {
		return moerr.NewInternalErrorNoCtx("substrait: lease journal record is too large")
	}
	return j.writeOnce(ctx, j.activePath(lease.Read.ReadRef), b)
}

func (j *FileServiceLeaseJournal) MarkReleased(ctx context.Context, readRef []byte) error {
	if len(readRef) != 32 {
		return moerr.NewInternalErrorNoCtx("substrait: invalid released read reference")
	}
	err := j.writeOnce(ctx, j.releasedPath(readRef), []byte{1})
	if moerr.IsMoErrCode(err, moerr.ErrFileAlreadyExists) {
		return nil
	}
	return err
}

func (j *FileServiceLeaseJournal) Delete(ctx context.Context, readRef []byte) error {
	if len(readRef) != 32 {
		return moerr.NewInternalErrorNoCtx("substrait: invalid deleted read reference")
	}
	for _, name := range []string{j.activePath(readRef), j.releasedPath(readRef)} {
		if err := j.fs.Delete(ctx, name); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return err
		}
	}
	return nil
}

func (j *FileServiceLeaseJournal) Load(ctx context.Context) ([]*Lease, error) {
	dir := path.Join(j.prefix, "active")
	var names []string
	active := make(map[string]struct{})
	for entry, err := range j.fs.List(ctx, dir) {
		if err != nil {
			return nil, err
		}
		if entry == nil || entry.IsDir || !strings.HasSuffix(entry.Name, ".json") {
			continue
		}
		if entry.Size <= 0 || entry.Size > maxJournalRecordSize {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: invalid lease journal record %q", entry.Name)
		}
		names = append(names, entry.Name)
		active[strings.TrimSuffix(entry.Name, ".json")] = struct{}{}
	}
	sort.Strings(names)
	result := make([]*Lease, 0, len(names))
	for _, name := range names {
		encoded := strings.TrimSuffix(name, ".json")
		readRef, err := hex.DecodeString(encoded)
		if err != nil || len(readRef) != 32 {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: invalid lease journal name %q", name)
		}
		b, err := j.read(ctx, path.Join(dir, name))
		if err != nil {
			return nil, err
		}
		var envelope journalEnvelope
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&envelope); err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: decode lease journal record %q: %v", name, err)
		}
		if err = ensureJSONEOF(decoder); err != nil {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: decode lease journal record %q: %v", name, err)
		}
		recordBytes, err := json.Marshal(envelope.Record)
		if err != nil {
			return nil, err
		}
		digest := sha256.Sum256(recordBytes)
		if !equalBytes(digest[:], envelope.SHA256) {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: lease journal checksum mismatch %q", name)
		}
		record := envelope.Record
		tr, err := UnmarshalTaeRead(record.Wire, 0)
		if err != nil || !equalBytes(tr.ReadRef, readRef) {
			return nil, moerr.NewInternalErrorNoCtxf("substrait: lease journal identity mismatch %q", name)
		}
		_, statErr := j.fs.StatFile(ctx, j.releasedPath(readRef))
		released := statErr == nil
		if statErr != nil && !moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound) {
			return nil, statErr
		}
		result = append(result, &Lease{Read: tr, Wire: record.Wire, Manifest: record.Manifest, CanonicalSchema: record.CanonicalSchema, AuthorizedClientSPKIHash: record.AuthorizedClientSPKIHash, ObjectNames: record.ObjectNames, Released: released})
	}
	for entry, err := range j.fs.List(ctx, path.Join(j.prefix, "released")) {
		if err != nil {
			return nil, err
		}
		if entry == nil || entry.IsDir {
			continue
		}
		if _, ok := active[entry.Name]; !ok {
			if err := j.fs.Delete(ctx, path.Join(j.prefix, "released", entry.Name)); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				return nil, err
			}
		}
	}
	return result, nil
}

func (j *FileServiceLeaseJournal) writeOnce(ctx context.Context, name string, data []byte) error {
	return j.fs.Write(ctx, fileservice.IOVector{FilePath: name, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(len(data)), Data: data}}})
}

func (j *FileServiceLeaseJournal) read(ctx context.Context, name string) ([]byte, error) {
	vector := fileservice.IOVector{FilePath: name, Entries: []fileservice.IOEntry{{Offset: 0, Size: -1}}}
	if err := j.fs.Read(ctx, &vector); err != nil {
		return nil, err
	}
	defer vector.Release()
	if len(vector.Entries) != 1 || len(vector.Entries[0].Data) == 0 || len(vector.Entries[0].Data) > maxJournalRecordSize {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease journal record size")
	}
	return append([]byte(nil), vector.Entries[0].Data...), nil
}

func (j *FileServiceLeaseJournal) activePath(readRef []byte) string {
	return path.Join(j.prefix, "active", hex.EncodeToString(readRef)+".json")
}

func (j *FileServiceLeaseJournal) releasedPath(readRef []byte) string {
	return path.Join(j.prefix, "released", hex.EncodeToString(readRef))
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return moerr.NewInternalErrorNoCtx("multiple JSON values")
		}
		return err
	}
	return nil
}
