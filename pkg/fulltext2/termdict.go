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
	"bytes"

	"github.com/blevesearch/vellum"
)

// termDict is the on-disk / loaded term dictionary of a segment: a vellum FST
// mapping each indexed term to a uint64 value (the term's posting-offset; df is
// read from the posting-list header). It is the compact, sorted realization of
// the backbone described in doc.go — the build side accumulates terms in a Go
// map (Segment.terms), then buildTermDictFST encodes the sorted keys into this
// FST for storage and query.
//
// vellum is byte-oriented, and UTF-8 byte order equals Unicode code-point order,
// so terms sorted with Go's ordinary string sort (Segment.sortedTerms) are in
// exactly the ascending byte order vellum's builder requires — Chinese / CJK
// terms need no special handling, and prefix iteration works on whole-token
// (character-boundary) prefixes.
type termDict struct {
	fst  *vellum.FST
	data []byte // backing bytes; vellum.Load references (does not copy) this
}

// buildTermDictFST encodes ascending (term → value) pairs into vellum FST bytes.
// terms must be in ascending byte order (Segment.sortedTerms) and unique, and
// len(values) must equal len(terms); values[i] is term[i]'s posting-offset.
func buildTermDictFST(terms []string, values []uint64) ([]byte, error) {
	var buf bytes.Buffer
	b, err := vellum.New(&buf, nil)
	if err != nil {
		return nil, err
	}
	for i, t := range terms {
		if err = b.Insert([]byte(t), values[i]); err != nil {
			return nil, err
		}
	}
	if err = b.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// loadTermDict wraps FST bytes (as produced by buildTermDictFST) for query. The
// returned dict references data directly, so data must outlive the dict; Close
// releases the FST.
func loadTermDict(data []byte) (*termDict, error) {
	fst, err := vellum.Load(data)
	if err != nil {
		return nil, err
	}
	return &termDict{fst: fst, data: data}, nil
}

// get returns the value (posting-offset) for an exact term, or ok=false if the
// term is absent.
func (d *termDict) get(term string) (val uint64, ok bool, err error) {
	return d.fst.Get([]byte(term))
}

// len returns the number of terms in the dictionary.
func (d *termDict) len() int { return d.fst.Len() }

// prefixIter returns an ascending iterator over every term that begins with
// prefix (the `word*` enumeration). An empty prefix iterates the whole dict. The
// caller must Close the iterator. A nil iterator with ok=false means the prefix
// matched nothing.
func (d *termDict) prefixIter(prefix string) (it *vellum.FSTIterator, ok bool, err error) {
	start := []byte(prefix)
	it, err = d.fst.Iterator(start, prefixSuccessor(start))
	if err == vellum.ErrIteratorDone {
		return nil, false, nil // no term with this prefix
	}
	if err != nil {
		return nil, false, err
	}
	return it, true, nil
}

// prefixSuccessor returns the smallest byte string strictly greater than every
// string having prefix as a prefix — i.e. the exclusive upper bound for a prefix
// range scan: copy the prefix, increment the last byte that is < 0xFF, and drop
// the trailing 0xFF bytes. Returns nil (an open upper bound → iterate to the end
// of the FST) when prefix is empty or all 0xFF, both of which have no finite
// successor.
func prefixSuccessor(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// prefixTerms collects every term with the given prefix into an ascending slice
// (the `word*` expansion, materialized). Empty prefix returns all terms.
func (d *termDict) prefixTerms(prefix string) ([]string, error) {
	it, ok, err := d.prefixIter(prefix)
	if err != nil || !ok {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	var out []string
	for {
		term, _ := it.Current()
		out = append(out, string(term))
		if err := it.Next(); err == vellum.ErrIteratorDone {
			break
		} else if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// forEachTerm streams every term (ascending) through fn WITHOUT materializing the whole
// vocabulary as a []string the way prefixTerms("") does — so a large high-cardinality
// index's MERGE reconstruction (forEachPosting) doesn't spike O(vocabulary) of strings.
func (d *termDict) forEachTerm(fn func(term string) error) error {
	it, ok, err := d.prefixIter("")
	if err != nil || !ok {
		return err
	}
	defer func() { _ = it.Close() }()
	for {
		term, _ := it.Current()
		if err := fn(string(term)); err != nil {
			return err
		}
		if err := it.Next(); err == vellum.ErrIteratorDone {
			break
		} else if err != nil {
			return err
		}
	}
	return nil
}

// Close releases the FST. Safe on a nil dict.
func (d *termDict) Close() error {
	if d == nil || d.fst == nil {
		return nil
	}
	return d.fst.Close()
}
