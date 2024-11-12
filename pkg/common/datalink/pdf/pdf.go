// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package pdf

import (
	"bytes"
	"strings"

	pdftotext "github.com/cpegeric/pdftotext-go"
	gopdf "github.com/dslipak/pdf"
)

var PDFTOTEXT_EXISTS bool = false

// stub function for UT
var pdftotext_extract = pdftotext.Extract
var pdftotext_check_version = pdftotext.CheckPopplerVersion

func init() {
	PDFTOTEXT_EXISTS = check_pdftotext()
}

func check_pdftotext() bool {
	_, err := pdftotext_check_version()
	return err == nil
}

func GetPlainTextFromPdfToText(data []byte) ([]byte, error) {

	pages, err := pdftotext_extract(data)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	for i, p := range pages {
		if i > 0 {
			buf.WriteString("\n")
		}
		buf.WriteString(p.Content)
	}

	return []byte(strings.TrimSpace(buf.String())), nil
}

func GetPlainText(data []byte) ([]byte, error) {

	if PDFTOTEXT_EXISTS {
		return GetPlainTextFromPdfToText(data)
	} else {
		return GetPlainTextFromDslipakPdf(data)
	}
}

func GetPlainTextFromDslipakPdf(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	pdfr, err := gopdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	npage := pdfr.NumPage()
	for i := 1; i <= npage; i++ {

		p := pdfr.Page(i)
		texts := p.Content().Text
		var lastY = 0.0
		line := ""

		for _, text := range texts {
			if lastY != text.Y {
				if lastY > 0 {
					buf.WriteString(line + "\n")
					line = text.S
				} else {
					line += text.S
				}
			} else {
				line += text.S
			}

			lastY = text.Y
		}
		buf.WriteString(line)
	}

	return []byte(strings.TrimSpace(buf.String())), nil
}
