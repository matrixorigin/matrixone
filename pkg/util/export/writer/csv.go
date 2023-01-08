// Copyright 2022 Matrix Origin
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

package writer

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"io"
	"strings"
)

var _ table.RowWriter = (*CSVWriter)(nil)

type CSVWriter struct {
	ctx    context.Context
	buf    *bytes.Buffer
	writer io.StringWriter
}

func NewCSVWriter(ctx context.Context, buf *bytes.Buffer, writer io.StringWriter) *CSVWriter {
	w := &CSVWriter{
		ctx:    ctx,
		buf:    buf,
		writer: writer,
	}
	return w
}

func (w *CSVWriter) WriteRow(row *table.Row) error {
	writeCsvOneLine(w.ctx, w.buf, row.ToStrings())
	return nil
}

func (w *CSVWriter) WriteStrings(record []string) error {
	writeCsvOneLine(w.ctx, w.buf, record)
	return nil
}

func (w *CSVWriter) GetContent() string {
	return w.buf.String()
}

func (w *CSVWriter) FlushAndClose() (int, error) {
	n, err := w.writer.WriteString(w.buf.String())
	if err != nil {
		return 0, err
	}
	w.writer = nil
	w.buf = nil
	return n, nil
}

func writeCsvOneLine(ctx context.Context, buf *bytes.Buffer, fields []string) {
	opts := table.CommonCsvOptions
	for idx, field := range fields {
		if idx > 0 {
			buf.WriteRune(opts.FieldTerminator)
		}
		if strings.ContainsRune(field, opts.FieldTerminator) || strings.ContainsRune(field, opts.EncloseRune) || strings.ContainsRune(field, opts.Terminator) {
			buf.WriteRune(opts.EncloseRune)
			QuoteFieldFunc(ctx, buf, field, opts.EncloseRune)
			buf.WriteRune(opts.EncloseRune)
		} else {
			buf.WriteString(field)
		}
	}
	buf.WriteRune(opts.Terminator)
}

var QuoteFieldFunc = func(ctx context.Context, buf *bytes.Buffer, value string, enclose rune) string {
	replaceRules := map[rune]string{
		'"':  `""`,
		'\'': `\'`,
	}
	quotedClose, hasRule := replaceRules[enclose]
	if !hasRule {
		panic(moerr.NewInternalError(ctx, "not support csv enclose: %c", enclose))
	}
	for _, c := range value {
		if c == enclose {
			buf.WriteString(quotedClose)
		} else {
			buf.WriteRune(c)
		}
	}
	return value
}
