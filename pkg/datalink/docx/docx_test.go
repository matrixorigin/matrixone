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
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Only Paragraph and Table contains text in docx file.  All text are enclosed with paragraph tag w:p for both paragraph and table element.
// 2 docx files included in the test and the sample xml files are in testfiles/sample.xml.
// test.docx only contains paragraphs.
// content of test.docx:
//
// This is a word file
//
// This is the heading of the file.
//
// This is a paragraph of text.
//
// This is another paragraph with text. some of it is italicized, and some of it is red. More is bold! What a lovely doc.
//
// content of test2.docx contains table:
//
// This is the heading of the file.
//
// This is a paragraph of text before the table.
//
// cell 1	cell 2	cell 3
// cell 4	cell 5	cell 6
//
// This is text after the table.

func TestOpenWordFileInvalidFile(t *testing.T) {

	_, err := openWordFile("testfiles/text.txt")
	if err == nil {
		t.Errorf("text files are not zip, should fail to open.")
	}
	if !strings.Contains(fmt.Sprintf("%s", err), "not a valid zip file") {
		t.Errorf("error message should be not a valid zip file")
	}

}

func TestOpenWordFileValidFile(t *testing.T) {

	doc, err := openWordFile("testfiles/test.docx")
	if err != nil {
		t.Errorf("failed to open a word file.")
	}
	if !strings.Contains(string(doc), "This is a word file") {
		t.Errorf("Error reading document.xml %s ", doc)
	}
	fmt.Printf("%s", err)
}

func TestParseText(t *testing.T) {

	_, err := ParseText("testfiles/text.txt")
	if err == nil {
		t.Errorf("parse should fail \n %s", err)
	}

	doctext, err := ParseText("testfiles/test.docx")
	if err != nil {
		t.Errorf("parsing test.docx should work \n %s", err)
	}

	// contains text at the begining of the document
	if !strings.Contains(doctext, "This is a word file") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	// contains text in the heading
	if !strings.Contains(doctext, "This is the heading of the file.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	// contains text that mix with different style such as bold, italicized and different color
	if !strings.Contains(doctext, "This is another paragraph with text. some of it is italicized, and some of it is red. More is bold! What a lovely doc.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	// contains text at the end of the document
	if !strings.Contains(doctext, "What a lovely doc.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	doctext2, err := ParseText("testfiles/test2.docx")
	if err != nil {
		t.Errorf("parsing test.docx should work \n %s", err)
	}

	// contain text right before the table
	if !strings.Contains(doctext2, "before the table") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext2)
	}

	// contain text right after the table
	if !strings.Contains(doctext2, "This is text after the table.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext2)
	}

	// contains table values
	for i := 1; i <= 6; i++ {
		tv := fmt.Sprintf("cell %d", i)
		if !strings.Contains(doctext2, tv) {
			t.Errorf("parsed text does not contain table value \n %s", doctext2)
		}
	}

	//fmt.Printf(doc)
}

func TestParseTextFromReader(t *testing.T) {

	dat, err := os.ReadFile("testfiles/text.txt")
	if err != nil {
		t.Errorf("read file error %s", err)
	}

	_, err = ParseTextFromReader(bytes.NewReader(dat), int64(len(dat)))
	if err == nil {
		t.Errorf("parse should fail \n %s", err)
	}

	dat, err = os.ReadFile("testfiles/test.docx")
	if err != nil {
		t.Errorf("read file error %s", err)
	}
	doctext, err := ParseTextFromReader(bytes.NewReader(dat), int64(len(dat)))
	if err != nil {
		t.Errorf("parsing test.docx should work \n %s", err)
	}

	// contains text at first paragraph
	if !strings.Contains(doctext, "This is a word file") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	// contains text in the heading
	if !strings.Contains(doctext, "This is the heading of the file.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	// contains text that mix with different style such as bold, italicized and different color
	if !strings.Contains(doctext, "This is another paragraph with text. some of it is italicized, and some of it is red. More is bold! What a lovely doc.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	// contains text at the end of the document
	if !strings.Contains(doctext, "What a lovely doc.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext)
	}

	dat, err = os.ReadFile("testfiles/test2.docx")
	if err != nil {
		t.Errorf("read file error %s", err)
	}
	doctext2, err := ParseTextFromReader(bytes.NewReader(dat), int64(len(dat)))
	if err != nil {
		t.Errorf("parsing test.docx should work \n %s", err)
	}

	// contains text right before the table
	if !strings.Contains(doctext2, "before the table") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext2)
	}
	// contains text right after the table
	if !strings.Contains(doctext2, "This is text after the table.") {
		t.Errorf("parsed text does not contain expected text \n %s", doctext2)
	}

	// contains table values
	for i := 1; i <= 6; i++ {
		tv := fmt.Sprintf("cell %d", i)
		if !strings.Contains(doctext2, tv) {
			t.Errorf("parsed text does not contain table value \n %s", doctext2)
		}
	}
	//fmt.Printf(doc)
}
