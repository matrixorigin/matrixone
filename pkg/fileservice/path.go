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

package fileservice

import (
	"encoding/csv"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const ServiceNameSeparator = ":"

type Path struct {
	Service          string
	ServiceArguments []string
	File             string
}

func (p Path) String() string {
	return JoinPath(
		p.ServiceString(),
		p.File,
	)
}

func (p Path) ServiceString() string {
	return strings.Join(append([]string{p.Service}, p.ServiceArguments...), ",")
}

// EncodeServiceCSV encodes a service record with the standard CSV writer's
// quoting and trailing LF, avoiding its streaming buffer for simple fields.
func EncodeServiceCSV(fields []string) (string, error) {
	size := 1
	for i, field := range fields {
		first, _ := utf8.DecodeRuneInString(field)
		// Bound the preflight scan; large or quoted fields use the standard writer.
		if len(field) > 4096 || field == `\.` || strings.ContainsAny(field, ",\"\r\n") || unicode.IsSpace(first) {
			var buf strings.Builder
			w := csv.NewWriter(&buf)
			if err := w.Write(fields); err != nil {
				return "", err
			}
			w.Flush()
			return buf.String(), w.Error()
		}
		size += len(field)
		if i > 0 {
			size++
		}
	}
	var buf strings.Builder
	buf.Grow(size)
	for i, field := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(field)
	}
	buf.WriteByte('\n')
	return buf.String(), nil
}

// EncodeServicePath formats decoded service fields and a file expression using
// the existing CSV path representation. No fields means a raw file expression.
func EncodeServicePath(fields []string, file string) (string, error) {
	if len(fields) == 0 {
		return file, nil
	}
	service, err := EncodeServiceCSV(fields)
	if err != nil {
		return "", err
	}
	return JoinPath(service, file), nil
}

// ParsePathFromServiceFields interprets decoded fields without serializing and
// reparsing them when the CSV round trip would preserve their values.
// Returned arguments do not alias the caller's field scratch.
func ParsePathFromServiceFields(fields []string, file string) (Path, error) {
	fallback := len(fields) == 0 || strings.Contains(file, ServiceNameSeparator) || len(fields) == 1 && fields[0] == ""
	for _, field := range fields {
		if strings.ContainsAny(field, "\r\n") {
			fallback = true
			break
		}
	}
	if fallback {
		// Preserve named/local paths, the last-colon grammar, CSV newline
		// normalization and the single empty record's existing behavior.
		path, err := EncodeServicePath(fields, file)
		if err != nil {
			return Path{}, err
		}
		return ParsePath(path)
	}
	// Match ParsePath's ownership: the returned arguments do not retain the
	// caller's scratch. Only this direct branch needs an argument allocation;
	// the compatibility parser already allocates its own arguments.
	arguments := make([]string, len(fields)-1)
	copy(arguments, fields[1:])
	return validatePath(Path{Service: fields[0], ServiceArguments: arguments, File: file})
}

// ParseETLService parses the provider without certifying the file expression.
// LOAD admission uses it before the existing glob expansion; concrete I/O uses ParsePath.
func ParseETLService(s string) (path Path, err error) {
	// split
	i := strings.LastIndex(s, ServiceNameSeparator)
	if i == -1 {
		// no service part
		path.File = s
	} else {
		// with service part
		path.Service, path.ServiceArguments, err = parseService(s[:i])
		if err != nil {
			return
		}
		path.File = s[i+1:]
	}

	return
}

func ParsePath(s string) (path Path, err error) {
	path, err = ParseETLService(s)
	if err != nil {
		return
	}
	return validatePath(path)
}

func validatePath(path Path) (Path, error) {
	// validate
	for _, r := range path.File {
		// most common patterns first
		if r >= '0' && r <= '9' ||
			r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' {
			continue
		}
		switch r {
		case '!', '-', '_', '.', '*', '\'', '(', ')', '@', '/', '=', '%':
			continue
		}
		// printable non-ASCII characters
		if r > unicode.MaxASCII && unicode.IsPrint(r) {
			continue
		}
		return path, moerr.NewInvalidPathNoCtx(path.File)
	}

	path.File = strings.TrimLeft(path.File, "/") // trim leading /

	return path, nil
}

func canonicalFilePaths(paths []string) ([]string, error) {
	canonical := make([]string, 0, len(paths))
	for _, value := range paths {
		path, err := ParsePath(value)
		if err != nil {
			return nil, err
		}
		canonical = append(canonical, path.File)
	}
	return canonical, nil
}

func parseService(str string) (service string, arguments []string, err error) {
	// A nonempty unquoted single record needs no CSV buffering. Keep the
	// original input for the CSV reader's quoting, blank-line and CRLF rules.
	record := strings.TrimSuffix(str, "\n")
	if record != "" && !strings.ContainsAny(record, "\"\r\n") {
		fields := strings.Split(record, ",")
		return fields[0], fields[1:], nil
	}
	r := csv.NewReader(strings.NewReader(str))
	records, err := r.ReadAll()
	if err != nil {
		return
	}
	if len(records) != 1 {
		err = moerr.NewInvalidInputNoCtxf("bad service: %v", str)
		return
	}
	service = records[0][0]
	arguments = records[0][1:]
	return
}

func ParsePathAtService(s string, serviceStr string) (path Path, err error) {
	path, err = ParsePath(s)
	if err != nil {
		return
	}
	if serviceStr != "" &&
		path.Service != "" &&
		!strings.EqualFold(path.ServiceString(), serviceStr) {
		err = moerr.NewWrongServiceNoCtx(serviceStr, path.Service)
		return
	}
	return
}

// parseFilePathAtService parses a path used by an operation that targets a
// file. Directory operations deliberately accept root paths; file operations
// must not reinterpret an empty or dot-only path as the root object.
func parseFilePathAtService(s string, serviceStr string) (path Path, err error) {
	path, err = ParsePathAtService(s, serviceStr)
	if err != nil {
		return
	}
	// path.Join and filepath.Join both collapse dot-only relative paths to the
	// current directory. Treat those spellings as the root too, otherwise
	// Delete(".") can remove an empty service or wrapper root.
	if path.File == "" || pathpkg.Clean(path.File) == "." {
		err = moerr.NewFileNotFoundNoCtx(path.File)
	}
	return
}

func JoinPath(serviceName string, path string) string {
	if len(serviceName) == 0 {
		return path
	}
	buf := new(strings.Builder)
	buf.WriteString(serviceName)
	buf.WriteString(ServiceNameSeparator)
	buf.WriteString(path)
	return buf.String()
}

var osPathSeparatorStr = string([]rune{os.PathSeparator})

func toOSPath(filePath string) string {
	if os.PathSeparator == '/' {
		return filePath
	}
	return strings.ReplaceAll(filePath, "/", osPathSeparatorStr)
}

func fromOSPath(diskPath string) string {
	diskPath = filepath.Clean(diskPath)
	if os.PathSeparator == '/' {
		return diskPath
	}
	return strings.ReplaceAll(diskPath, osPathSeparatorStr, "/")
}
