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

import "strings"

const BM25_K1 = 1.5
const BM25_B = 0.75

const DOC_LEN_WORD = "__DocLen" //

type FullTextScoreAlgo int

const (
	ALGO_BM25 FullTextScoreAlgo = iota
	ALGO_TFIDF
)

const (
	FulltextRelevancyAlgo       = "ft_relevancy_algorithm"
	FulltextRelevancyAlgo_bm25  = "BM25"
	FulltextRelevancyAlgo_tfidf = "TF-IDF"
)

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


# Step 1
Pattern/Plan is generated from search string.

In boolean mode,

search string: 'Matrix Origin' -> pattern: "(text 0 matrix) (text 1 origin)"
search string: '"Matrix Origin"' -> pattern: "(phrase (text 0 0 matrix) (text 1 7 origin))"
search string: '+Matrix +Origin' -> pattern: "(join 0 (+ (text 0 matrix)) (+ (text 0 origin)))"
search string: '+读写汉字 -学中文' -> pattern: "(+ (text 0 读写汉字)) (- (text 1 学中文))"
search string: 'Matrix +(<Origin >One)' -> pattern: "(+ (group (< (text 0 origin)) (> (text 1 one)))) (text 2 matrix)"

In natural language mode,

search string: '读写汉字 学中文' -> pattern: "(text 0 0 读写汉) (text 1 3 写汉字) (* 2 6 汉字*) (* 3 9 字*) (text 4 13 学中文) (* 5 16 中文*) (* 6 19 文*)"

# Step 2

Generate the SQL from pattern

# Step 3

Run the SQL and get the statistics

# Step 4

Run Eval() to get final answer and score

*/

// Parser parameters
type FullTextParserParam struct {
	Parser string `json:"parser"`
}

// Search accumulator is to parse the search string into list of pattern and each pattern will associate with WordAccum by pattern.Text
type SearchAccum struct {
	SrcTblName string
	TblName    string
	Mode       int64
	Pattern    []*Pattern
	Params     string
	Nrow       int64
	Nkeywords  int

	ScoreAlgo FullTextScoreAlgo
	AvgDocLen float64
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
	JOIN        = 9
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
	case JOIN:
		return "join"
	default:
		return ""
	}
}

// Pattern works on both Natural Language and Boolean mode
type Pattern struct {
	Text     string
	Operator int
	Children []*Pattern
	Position int32
	Index    int32
}

func PatternListToString(ps []*Pattern) string {
	ss := make([]string, 0, len(ps))
	for _, p := range ps {
		ss = append(ss, p.String())
	}

	return strings.Join(ss, " ")
}

func PatternToString(pattern string, mode int64) (string, error) {
	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return "", err
	}

	return PatternListToString(ps), nil
}

func PatternListToStringWithPosition(ps []*Pattern) string {
	ss := make([]string, 0, len(ps))
	for _, p := range ps {
		ss = append(ss, p.StringWithPosition())
	}

	return strings.Join(ss, " ")
}

func PatternToStringWithPosition(pattern string, mode int64) (string, error) {
	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return "", err
	}

	return PatternListToStringWithPosition(ps), nil
}
