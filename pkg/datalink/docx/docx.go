// Copyright 2022 Matrix Origin
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

package docx

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func ParseText(filename string) (string, error) {

	doc, err := openWordFile(filename)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtx(fmt.Sprintf("Error opening file %s - %s", filename, err))
	}

	docx, err := Parse(doc)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtx(fmt.Sprintf("Error parsing %s - %s", filename, err))
	}

	return docx.AsText(), nil

}

func Parse(doc []byte) (WordDocument, error) {

	docx := WordDocument{}
	r := strings.NewReader(string(doc))
	decoder := xml.NewDecoder(r)

	for {
		t, _ := decoder.Token()
		if t == nil {
			break
		}
		switch se := t.(type) {
		case xml.StartElement:
			if se.Name.Local == "p" {
				var p Paragraph
				decoder.DecodeElement(&p, &se)
				docx.Paragraphs = append(docx.Paragraphs, p)
			}
		}
	}
	return docx, nil
}

func openWordFileFromReader(reader io.ReaderAt, size int64) ([]byte, error) {

	r, err := zip.NewReader(reader, size)
	if err != nil {
		return nil, err
	}

	// Iterate through the files in the archive,
	// find document.xml
	for _, f := range r.File {

		//fmt.Printf("Contents of %s:\n", f.Name)
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		if f.Name == "word/document.xml" {
			doc, err := io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
			return doc, nil
		}
		rc.Close()
	}

	return nil, nil
}

func ParseTextFromReader(reader io.ReaderAt, size int64) (string, error) {

	doc, err := openWordFileFromReader(reader, size)
	if err != nil {
		return "", err
	}

	docx, err := Parse(doc)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtx(fmt.Sprintf("Error parsing %s", err))
	}

	return docx.AsText(), nil
}

func openWordFile(filename string) ([]byte, error) {

	// Open a zip archive for reading. word files are zip archives
	r, err := zip.OpenReader(filename)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	// Iterate through the files in the archive,
	// find document.xml
	for _, f := range r.File {

		//fmt.Printf("Contents of %s:\n", f.Name)
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		if f.Name == "word/document.xml" {
			doc, err := io.ReadAll(rc)
			if err != nil {
				return nil, err
			}
			return doc, nil
		}
		rc.Close()
	}

	return nil, nil
}

func GetPlainText(data []byte) ([]byte, error) {
	doc, err := ParseTextFromReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}
	return []byte(doc), err
}
