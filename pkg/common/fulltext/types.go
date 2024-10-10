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

package fulltext

/*
The following examples demonstrate some search strings that use boolean full-text operators:

'apple banana'

Find rows that contain at least one of the two words.

'+apple +juice'

Find rows that contain both words.

'+apple macintosh'

Find rows that contain the word “apple”, but rank rows higher if they also contain “macintosh”.

'+apple -macintosh'

Find rows that contain the word “apple” but not “macintosh”.

'+apple ~macintosh'

Find rows that contain the word “apple”, but if the row also contains the word “macintosh”, rate it lower than if row does not. This is “softer” than a search for '+apple -macintosh', for which the presence of “macintosh” causes the row not to be returned at all.

'+apple +(>turnover <strudel)'

Find rows that contain the words “apple” and “turnover”, or “apple” and “strudel” (in any order), but rank “apple turnover” higher than “apple strudel”.

'apple*'

Find rows that contain words such as “apple”, “apples”, “applesauce”, or “applet”.

'"some words"'

Find rows that contain the exact phrase “some words” (for example, rows that contain “some words of wisdom” but not “some noise words”). Note that the " characters that enclose the phrase are operator characters that delimit the phrase. They are not the quotation marks that enclose the search string itself.
*/

// Parser parameters
type FullTextParserParam struct {
	Parser string `json:"parser"`
}

// Word is associated with particular DocId (index.doc_id) and could have multiple positions
type Word struct {
	DocId    any
	Position []int32
	DocCount int32
}

// Word accumulator accumulate the same word appeared in multiple (index.doc_id).
type WordAccum struct {
	Words map[any]*Word
}

// Search accumulator is to parse the search string into list of pattern and each pattern will associate with WordAccum by pattern.Text
type SearchAccum struct {
	SrcTblName string
	TblName    string
	Mode       int64
	Pattern    []*Pattern
	Params     string
	WordAccums map[string]*WordAccum
	Nrow       int64
}

// Boolean mode search string parsing
type FullTextBooleanOperator int

var (
	TEXT        = 0
	STAR        = 1
	PLUS        = 2
	MINUS       = 3
	LESSTHAN    = 4
	GREATERTHAN = 5
	RANKLESS    = 6
	GROUP       = 7
	PHRASE      = 8
)

func OperatorToString(op int) string {
	switch op {
	case TEXT:
		return "text"
	case STAR:
		return "*"
	case PLUS:
		return "+"
	case MINUS:
		return "-"
	case LESSTHAN:
		return "<"
	case GREATERTHAN:
		return ">"
	case RANKLESS:
		return "~"
	case GROUP:
		return "group"
	case PHRASE:
		return "phrase"
	default:
		return ""
	}
}

// Pattern works on both Natural Language and Boolean mode
type Pattern struct {
	Text     string
	Operator int
	Children []*Pattern
}
