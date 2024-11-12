// Copyright 2024 Matrix Origin
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

package pdf

import (
	"os"
	"testing"

	pdftotext "github.com/cpegeric/pdftotext-go"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func reset() {
	pdftotext_extract = pdftotext.Extract
	pdftotext_check_version = pdftotext.CheckPopplerVersion
}

func TestCheckVersion(t *testing.T) {
	defer reset()

	pdftotext_check_version = func() (string, error) {
		return "1.0", nil
	}

	exists := check_pdftotext()
	require.True(t, exists)

	pdftotext_check_version = func() (string, error) {
		return "", moerr.NewInternalErrorNoCtx("file not found")
	}

	exists = check_pdftotext()
	require.False(t, exists)
}

func TestError(t *testing.T) {

	defer reset()

	pdftotext_extract = func(data []byte) ([]pdftotext.PdfPage, error) {
		return nil, moerr.NewInternalErrorNoCtx("some error")
	}
	_, err := GetPlainTextFromPdfToText(nil)
	require.NotNil(t, err)
}

func TestPdfToText(t *testing.T) {

	defer reset()

	bytes, err := os.ReadFile("test/test3.pdf")
	require.Nil(t, err)

	pdftotext_extract = func(data []byte) ([]pdftotext.PdfPage, error) {
		return []pdftotext.PdfPage{{Content: "Happy Birthday", Number: 1}, {Content: "Merry Christmas", Number: 2}}, nil
	}

	data, err := GetPlainTextFromPdfToText(bytes)
	require.Nil(t, err)

	require.Equal(t, "Happy Birthday\nMerry Christmas", string(data))
}

func TestGoPdf(t *testing.T) {

	bytes, err := os.ReadFile("test/test3.pdf")
	require.Nil(t, err)

	data, err := GetPlainTextFromDslipakPdf(bytes)
	require.Nil(t, err)

	require.Equal(t, string(data), "OptimizingMySQL','Inthistutorial,weshow...")
}

func TestPdf(t *testing.T) {

	pdftotext_exist := PDFTOTEXT_EXISTS

	defer func() {
		pdftotext_extract = pdftotext.Extract
		pdftotext_check_version = pdftotext.CheckPopplerVersion
		PDFTOTEXT_EXISTS = pdftotext_exist
	}()

	bytes, err := os.ReadFile("test/test3.pdf")
	require.Nil(t, err)

	PDFTOTEXT_EXISTS = false
	data, err := GetPlainText(bytes)
	require.Nil(t, err)
	require.Equal(t, string(data), "OptimizingMySQL','Inthistutorial,weshow...")

	PDFTOTEXT_EXISTS = true
	pdftotext_extract = func(data []byte) ([]pdftotext.PdfPage, error) {
		return []pdftotext.PdfPage{{Content: "Happy Birthday", Number: 1}, {Content: "Merry Christmas", Number: 2}}, nil
	}
	data, err = GetPlainText(bytes)
	require.Nil(t, err)
	require.Equal(t, string(data), "Happy Birthday\nMerry Christmas")
}

/*
func TestRealPdfToText(t *testing.T) {
	bytes, err := os.ReadFile("test/test3.pdf")
	require.Nil(t, err)

	data, err := GetPlainText(bytes)
	require.Nil(t, err)
	require.Equal(t, "Optimizing MySQL','In this tutorial, we show ...", string(data))

}
*/
