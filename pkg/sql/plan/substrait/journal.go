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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// JSON base64-expands the 64 MiB manifest and 1 MiB schema by 4/3. The
// journal also carries the canonical wire and the manifest's object names.
const maxJournalRecordSize = 160 << 20

const (
	journalHeaderMagic      = "MO-SUBSTRAIT-LEASE\x01"
	journalHeaderSize       = len(journalHeaderMagic) + sha256.Size
	journalCleanupBatchSize = 128
)

// FileServiceLeaseJournal persists leases in the shared file service. The
// immutable record contains the payload while a separate write-once authority
// object is the lease's single active state. Terminal release atomically
// deletes that authority before GC protection is removed.
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
	record := journalRecord{Wire: lease.Wire, Manifest: lease.Manifest, CanonicalSchema: lease.CanonicalSchema, AuthorizedClientSPKIHash: lease.AuthorizedClientSPKIHash, ObjectNames: lease.ObjectNames}
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(recordBytes)
	payload, err := json.Marshal(journalEnvelope{Record: record, SHA256: digest[:]})
	if err != nil {
		return err
	}
	if len(payload)+journalHeaderSize > maxJournalRecordSize {
		return moerr.NewInternalErrorNoCtx("substrait: lease journal record is too large")
	}
	authority := leaseAuthorityDigest(lease)
	b := make([]byte, 0, journalHeaderSize+len(payload))
	b = append(b, journalHeaderMagic...)
	b = append(b, authority[:]...)
	b = append(b, payload...)
	// Publish the authority first, then the immutable record. A concurrent
	// replay cannot treat a partially written record as released, and the final
	// read detects orphan cleanup or revocation that raced publication.
	if err := j.writeOnce(ctx, j.authorityPath(lease.Read.ReadRef), authority[:]); err != nil {
		return err
	}
	if err := j.writeOnce(ctx, j.activePath(lease.Read.ReadRef), b); err != nil {
		return err
	}
	activeAuthority, err := j.readAuthority(ctx, lease.Read.ReadRef)
	if err != nil {
		return err
	}
	if !equalBytes(activeAuthority, authority[:]) {
		return moerr.NewInternalErrorNoCtx("substrait: lease journal authority changed during store")
	}
	return nil
}

// Active validates the immutable record before reading the single authority
// object. The final authority read is the durable linearization point against
// MarkReleased: it either precedes the atomic delete or observes revocation.
func (j *FileServiceLeaseJournal) Active(ctx context.Context, lease *Lease) (bool, error) {
	if lease == nil || lease.Read == nil || len(lease.Read.ReadRef) != 32 {
		return false, moerr.NewInternalErrorNoCtx("substrait: invalid active read lease")
	}
	header, err := j.readHeader(ctx, j.activePath(lease.Read.ReadRef))
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if string(header[:len(journalHeaderMagic)]) != journalHeaderMagic {
		return false, moerr.NewInternalErrorNoCtx("substrait: invalid lease journal header")
	}
	want := leaseAuthorityDigest(lease)
	if !equalBytes(header[len(journalHeaderMagic):], want[:]) {
		return false, nil
	}
	authority, err := j.readAuthority(ctx, lease.Read.ReadRef)
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return equalBytes(authority, want[:]), nil
}

func (j *FileServiceLeaseJournal) MarkReleased(ctx context.Context, readRef []byte) error {
	if len(readRef) != 32 {
		return moerr.NewInternalErrorNoCtx("substrait: invalid released read reference")
	}
	err := j.fs.Delete(ctx, j.authorityPath(readRef))
	if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
		return nil
	}
	return err
}

func (j *FileServiceLeaseJournal) Delete(ctx context.Context, readRef []byte) error {
	if len(readRef) != 32 {
		return moerr.NewInternalErrorNoCtx("substrait: invalid deleted read reference")
	}
	for _, name := range []string{j.authorityPath(readRef), j.activePath(readRef)} {
		if err := j.fs.Delete(ctx, name); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return err
		}
	}
	return nil
}

func (j *FileServiceLeaseJournal) Load(ctx context.Context, visit func(*Lease) error) error {
	if visit == nil {
		return moerr.NewInternalErrorNoCtx("substrait: missing lease journal visitor")
	}
	dir := path.Join(j.prefix, "active")
	for entry, err := range j.fs.List(ctx, dir) {
		if err != nil {
			return err
		}
		if entry == nil || entry.IsDir || !strings.HasSuffix(entry.Name, ".json") {
			continue
		}
		if entry.Size <= 0 || entry.Size > maxJournalRecordSize {
			return moerr.NewInternalErrorNoCtxf("substrait: invalid lease journal record %q", entry.Name)
		}
		name := entry.Name
		encoded := strings.TrimSuffix(name, ".json")
		readRef, err := hex.DecodeString(encoded)
		if err != nil || len(readRef) != 32 || hex.EncodeToString(readRef) != encoded {
			return moerr.NewInternalErrorNoCtxf("substrait: invalid lease journal name %q", name)
		}
		b, err := j.read(ctx, path.Join(dir, name))
		if err != nil {
			return err
		}
		if len(b) <= journalHeaderSize || string(b[:len(journalHeaderMagic)]) != journalHeaderMagic {
			return moerr.NewInternalErrorNoCtxf("substrait: invalid lease journal header %q", name)
		}
		authority := b[len(journalHeaderMagic):journalHeaderSize]
		var envelope journalEnvelope
		decoder := json.NewDecoder(bytes.NewReader(b[journalHeaderSize:]))
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&envelope); err != nil {
			return moerr.NewInternalErrorNoCtxf("substrait: decode lease journal record %q: %v", name, err)
		}
		if err = ensureJSONEOF(decoder); err != nil {
			return moerr.NewInternalErrorNoCtxf("substrait: decode lease journal record %q: %v", name, err)
		}
		recordBytes, err := json.Marshal(envelope.Record)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(recordBytes)
		if !equalBytes(digest[:], envelope.SHA256) {
			return moerr.NewInternalErrorNoCtxf("substrait: lease journal checksum mismatch %q", name)
		}
		record := envelope.Record
		tr, err := UnmarshalTaeRead(record.Wire, 0)
		if err != nil || !equalBytes(tr.ReadRef, readRef) {
			return moerr.NewInternalErrorNoCtxf("substrait: lease journal identity mismatch %q", name)
		}
		lease := &Lease{Read: tr, Wire: record.Wire, Manifest: record.Manifest, CanonicalSchema: record.CanonicalSchema, AuthorizedClientSPKIHash: record.AuthorizedClientSPKIHash, ObjectNames: record.ObjectNames}
		wantAuthority := leaseAuthorityDigest(lease)
		if !equalBytes(authority, wantAuthority[:]) {
			return moerr.NewInternalErrorNoCtxf("substrait: lease journal authority mismatch %q", name)
		}
		activeAuthority, authorityErr := j.readAuthority(ctx, readRef)
		if authorityErr != nil && !moerr.IsMoErrCode(authorityErr, moerr.ErrFileNotFound) {
			return authorityErr
		}
		lease.Released = moerr.IsMoErrCode(authorityErr, moerr.ErrFileNotFound)
		if authorityErr == nil && !equalBytes(activeAuthority, wantAuthority[:]) {
			return moerr.NewInternalErrorNoCtxf("substrait: lease journal authority state mismatch %q", name)
		}
		if err := visit(lease); err != nil {
			return err
		}
	}
	return j.cleanOrphanAuthorities(ctx)
}

// cleanOrphanAuthorities never retains the journal namespace. A fixed-size
// batch also keeps deletion outside FileService.List callbacks, which may hold
// an implementation read lock while yielding entries.
func (j *FileServiceLeaseJournal) cleanOrphanAuthorities(ctx context.Context) error {
	dir := path.Join(j.prefix, "authority")
	for {
		orphans := make([]string, 0, journalCleanupBatchSize)
		for entry, err := range j.fs.List(ctx, dir) {
			if err != nil {
				return err
			}
			if entry == nil || entry.IsDir {
				continue
			}
			readRef, decodeErr := hex.DecodeString(entry.Name)
			if decodeErr != nil || len(readRef) != 32 || hex.EncodeToString(readRef) != entry.Name {
				return moerr.NewInternalErrorNoCtxf("substrait: invalid lease journal authority name %q", entry.Name)
			}
			if _, statErr := j.fs.StatFile(ctx, j.activePath(readRef)); statErr == nil {
				continue
			} else if !moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound) {
				return statErr
			}
			orphans = append(orphans, j.authorityPath(readRef))
			if len(orphans) == journalCleanupBatchSize {
				break
			}
		}
		if len(orphans) == 0 {
			return nil
		}
		for _, name := range orphans {
			encoded := path.Base(name)
			readRef, err := hex.DecodeString(encoded)
			if err != nil {
				return err
			}
			// Store writes authority before the record. Recheck outside List so
			// a record that completed in the meantime keeps its authority.
			if _, statErr := j.fs.StatFile(ctx, j.activePath(readRef)); statErr == nil {
				continue
			} else if !moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound) {
				return statErr
			}
			if err := j.fs.Delete(ctx, name); err != nil && !moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
				return err
			}
		}
	}
}

func leaseAuthorityDigest(lease *Lease) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("matrixone/substrait/read-lease-authority/v1\x00"))
	_, _ = h.Write(lease.Wire)
	_, _ = h.Write(lease.AuthorizedClientSPKIHash)
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
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

func (j *FileServiceLeaseJournal) readHeader(ctx context.Context, name string) ([]byte, error) {
	vector := fileservice.IOVector{FilePath: name, Entries: []fileservice.IOEntry{{Offset: 0, Size: int64(journalHeaderSize)}}}
	if err := j.fs.Read(ctx, &vector); err != nil {
		return nil, err
	}
	defer vector.Release()
	if len(vector.Entries) != 1 || len(vector.Entries[0].Data) != journalHeaderSize {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease journal header size")
	}
	return append([]byte(nil), vector.Entries[0].Data...), nil
}

func (j *FileServiceLeaseJournal) readAuthority(ctx context.Context, readRef []byte) ([]byte, error) {
	vector := fileservice.IOVector{FilePath: j.authorityPath(readRef), Entries: []fileservice.IOEntry{{Offset: 0, Size: sha256.Size}}}
	if err := j.fs.Read(ctx, &vector); err != nil {
		return nil, err
	}
	defer vector.Release()
	if len(vector.Entries) != 1 || len(vector.Entries[0].Data) != sha256.Size {
		return nil, moerr.NewInternalErrorNoCtx("substrait: invalid lease journal authority size")
	}
	return append([]byte(nil), vector.Entries[0].Data...), nil
}

func (j *FileServiceLeaseJournal) activePath(readRef []byte) string {
	return path.Join(j.prefix, "active", hex.EncodeToString(readRef)+".json")
}

func (j *FileServiceLeaseJournal) authorityPath(readRef []byte) string {
	return path.Join(j.prefix, "authority", hex.EncodeToString(readRef))
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
