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

import (
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

/*
  1. Parse the search string into list of pattern []*Pattern
  2. With list of pattern, run SQL to get all pattern stats and store in SearchAccum/WordAccum.
  3. foreach pattern in the list, call Eval() function to compute the rank score based on previous rank score and the accumulate of the current pattern
     and return rank score as result

     i.e.

     result := nil
     for p := range searchAccum.Pattern {
            wordAccum := searchAccum.WordAccums[p.Text]
            result = p.Eval(result, wordAccum)
     }
   4. return result as answer
*/

func NewWordAccum() *WordAccum {
	return &WordAccum{Words: make(map[any]*Word)}
}

func NewSearchAccum(srctbl string, tblname string, pattern string, mode int64, params string) (*SearchAccum, error) {

	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return nil, err
	}

	return &SearchAccum{SrcTblName: srctbl, TblName: tblname, Mode: mode, Pattern: ps, Params: params, WordAccums: make(map[string]*WordAccum)}, nil
}

func findPatternByOperator(ps []*Pattern, op int) []*Pattern {
	var result []*Pattern

	for _, p := range ps {
		if p.Operator == op {
			result = append(result, p)
		}
	}

	return result
}

func findValuePattern(ps []*Pattern) []*Pattern {
	var result []*Pattern

	for _, p := range ps {
		if p.Operator != PLUS && p.Operator != MINUS {
			result = append(result, p)
		}
	}

	return result
}

func (s *SearchAccum) PatternAnyPlus() bool {
	for _, p := range s.Pattern {
		if p.Operator == PLUS {
			return true
		}
	}
	return false
}

// Evaluate the search string
func (s *SearchAccum) Eval() (map[any]float32, error) {
	var result map[any]float32
	var err error

	if s.Nrow == 0 {
		return result, nil
	}

	for _, p := range s.Pattern {
		result, err = p.Eval(s, float32(1.0), result)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// Pattern
func (p *Pattern) String() string {
	if p.Operator == TEXT || p.Operator == STAR {
		return fmt.Sprintf("(%s %s)", OperatorToString(p.Operator), p.Text)
	}

	str := fmt.Sprintf("(%s ", OperatorToString(p.Operator))
	for i, c := range p.Children {
		if i > 0 {
			str += " "
		}
		str += c.String()

	}
	str += ")"
	return str
}

func (p *Pattern) StringWithPosition() string {
	if p.Operator == TEXT || p.Operator == STAR {
		return fmt.Sprintf("(%s %d %s)", OperatorToString(p.Operator), p.Position, p.Text)
	}

	str := fmt.Sprintf("(%s ", OperatorToString(p.Operator))
	for i, c := range p.Children {
		if i > 0 {
			str += " "
		}
		str += c.StringWithPosition()

	}
	str += ")"
	return str
}

// Get the text of leaf nodes with operator specified. Either TEXT or STAR as operator.
func (p *Pattern) GetLeafText(operator int) []string {
	if p.Operator == operator {
		return []string{p.Text}
	}

	var res []string
	for _, c := range p.Children {
		res = append(res, c.GetLeafText(operator)...)
	}
	return res
}

// Eval leaf node.  compute the tfidf from the data in WordAccums and return result as map[doc_id]float32
func (p *Pattern) EvalLeaf(s *SearchAccum, weight float32, result map[any]float32) (map[any]float32, error) {
	key := p.Text
	acc, ok := s.WordAccums[key]
	if !ok {
		// never return nil result
		result = make(map[any]float32)
		return result, nil
	}

	if result == nil {
		result = make(map[any]float32)
	}

	nmatch := float64(len(acc.Words))
	idf := math.Log10(float64(s.Nrow) / nmatch)
	idfSq := float32(idf * idf)
	for doc_id := range acc.Words {
		tf := float32(acc.Words[doc_id].DocCount)
		result[doc_id] = weight * tf * idfSq
	}

	return result, nil
}

// Eval Plus Plus operation.  Basically AND operation between input argument and result from the previous Eval()
// e.g. (+ (text apple)) (+ (text banana))
func (p *Pattern) EvalPlusPlus(s *SearchAccum, arg, result map[any]float32) (map[any]float32, error) {
	if result == nil {
		result = make(map[any]float32)
		return result, nil
	}

	keys := make([]any, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	for _, doc_id := range keys {
		_, ok := arg[doc_id]
		if ok {
			result[doc_id] += arg[doc_id]
		} else {
			delete(result, doc_id)
		}
	}
	return result, nil
}

// Eval Plus OR.  The previous result from Eval() is a Plus Operator and current Pattern is a Text or Star.
// e.g. (+ (text apple)) (text banana)
func (p *Pattern) EvalPlusOR(s *SearchAccum, arg, result map[any]float32) (map[any]float32, error) {
	if result == nil {
		result = make(map[any]float32)
		return result, nil
	}

	keys := make([]any, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	for _, doc_id := range keys {
		_, ok := arg[doc_id]
		if ok {
			result[doc_id] += arg[doc_id]
		}
	}
	return result, nil
}

// Minus operation.  Remove the result when doc_id is present in argument
// e.g. (+ (text apple)) (- (text banana))
func (p *Pattern) EvalMinus(s *SearchAccum, arg, result map[any]float32) (map[any]float32, error) {
	if result == nil {
		result = make(map[any]float32)
		return result, nil
	}

	for doc_id := range arg {
		_, ok := result[doc_id]
		if ok {
			// remove from result
			delete(result, doc_id)
		}
	}
	return result, nil
}

// OR operation. Either apple and banana can be the result
// e.g. (text apple) (text banana)
func (p *Pattern) EvalOR(s *SearchAccum, arg, result map[any]float32) (map[any]float32, error) {
	if result == nil {
		result = make(map[any]float32)
	}

	for doc_id := range arg {
		_, ok := result[doc_id]
		if ok {
			result[doc_id] += arg[doc_id]
		} else {
			result[doc_id] = arg[doc_id]
		}
	}

	return result, nil
}

func (p *Pattern) EvalPhrase(s *SearchAccum, arg map[any]float32) (map[any]float32, error) {
	// check word order here

	result := make(map[any]float32)

	for docid := range arg {
		var pos []int32
		for j, c := range p.Children {
			wacc := s.WordAccums[c.Text]
			word := wacc.Words[docid]
			currpos := word.Position
			if j == 0 {
				pos = word.Position
			} else {
				retpos := make([]int32, 0, len(pos))
				// always compare with the first word offset '0'
				for _, p1 := range pos {
					for _, p2 := range currpos {
						diff := p2 - p1
						if c.Position == diff {
							retpos = append(retpos, p1)
						}
					}
				}
				if len(retpos) == 0 {
					pos = nil
					break
				} else {
					pos = retpos
				}
			}
		}

		if len(pos) > 0 {
			result[docid] = arg[docid]
		}
	}

	return result, nil
}

// Get the weight for compute the TFIDF
// LESSTHAN is lower the ranking
// GREATERTHAN is higher the ranking
// RANKLESS is to discourage the ranking but not delete such as Minus. weight is negative to discourage the ranking.
func (p *Pattern) GetWeight() float32 {
	switch p.Operator {
	case LESSTHAN:
		return float32(0.9)
	case GREATERTHAN:
		return float32(1.1)
	case RANKLESS:
		return float32(-1.0)
	default:
		return float32(1.0)
	}
}

// Combine two score maps into single map. max(float32) will return when same doc_id (key) exists in both arg and result.
func (p *Pattern) Combine(s *SearchAccum, arg, result map[any]float32) (map[any]float32, error) {
	if result == nil {
		return arg, nil
	}
	for k1 := range arg {
		v1 := arg[k1]
		v, ok := result[k1]
		if ok {
			// max
			if v1 > v {
				result[k1] = v1
			}
		} else {
			result[k1] = v1
		}
	}
	return result, nil
}

// Eval() function to evaluate the previous result from Eval and the current pattern (with data from datasource)  and return map[doc_id]float32
func (p *Pattern) Eval(accum *SearchAccum, weight float32, result map[any]float32) (map[any]float32, error) {
	switch p.Operator {
	case TEXT, STAR:
		// leaf node: TEXT, STAR
		// calculate the score with weight
		if result == nil {
			return p.EvalLeaf(accum, weight, result)
		} else {
			child_result, err := p.EvalLeaf(accum, weight, nil)
			if err != nil {
				return nil, err
			}
			if accum.PatternAnyPlus() {
				return p.EvalPlusOR(accum, child_result, result)
			} else {
				return p.EvalOR(accum, child_result, result)
			}
		}

	case PLUS:
		if result == nil {
			return p.Children[0].Eval(accum, weight, nil)
		} else {
			child_result, err := p.Children[0].Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			return p.EvalPlusPlus(accum, child_result, result)
		}
	case MINUS:
		if result == nil {
			result = make(map[any]float32)
			return result, nil
		} else {
			child_result, err := p.Children[0].Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			return p.EvalMinus(accum, child_result, result)
		}

	case LESSTHAN, GREATERTHAN:
		// get weight by type
		weight *= p.GetWeight()

		if result == nil {
			return p.Children[0].Eval(accum, weight, nil)
		} else {
			child_result, err := p.Children[0].Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			return p.EvalOR(accum, child_result, result)
		}
	case RANKLESS:
		// get weight by type
		weight *= p.GetWeight()

		if result == nil {
			return p.Children[0].Eval(accum, weight, nil)
		} else {
			child_result, err := p.Children[0].Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			// OR
			if accum.PatternAnyPlus() {
				return p.EvalPlusOR(accum, child_result, result)
			} else {
				return p.EvalOR(accum, child_result, result)
			}
		}
	case GROUP:
		result := make(map[any]float32)
		for _, c := range p.Children {
			child_result, err := c.Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			// COMBINE results from children
			result, err = p.Combine(accum, child_result, result)
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	case PHRASE:
		// all children are TEXT and AND operations
		for i, c := range p.Children {
			child_result, err := c.Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			if i == 0 {
				result = child_result
			} else {
				// AND operators with the results
				result, err = c.EvalPlusPlus(accum, child_result, result)
				if err != nil {
					return nil, err
				}
			}
		}

		// check word order
		return p.EvalPhrase(accum, result)
	default:
		return nil, moerr.NewInternalErrorNoCtx("Eval() not handled")
	}
}

// validate the Pattern
func (p *Pattern) Validate() error {
	switch p.Operator {
	case PLUS, MINUS:
		if len(p.Children) != 1 {
			return moerr.NewInternalErrorNoCtx("+/- must have single child with value")
		}
		for _, c := range p.Children {
			if c.Operator == PLUS || c.Operator == MINUS || c.Operator == PHRASE {
				return moerr.NewInternalErrorNoCtx("double +/- operator")
			}
		}

		for _, c := range p.Children {
			err := c.Validate()
			if err != nil {
				return err
			}
		}

	case TEXT, STAR:
		if len(p.Children) > 0 {
			return moerr.NewInternalErrorNoCtx("text Pattern cannot have children")
		}
	case PHRASE:
		for _, c := range p.Children {
			if c.Operator != TEXT {
				return moerr.NewInternalErrorNoCtx("PHRASE can only have text Pattern")
			}
		}
	case GROUP:
		if len(p.Children) == 0 {
			return moerr.NewInternalErrorNoCtx("sub-query is empty")
		}

		for _, c := range p.Children {
			if c.Operator == PLUS || c.Operator == MINUS || c.Operator == PHRASE {
				return moerr.NewInternalErrorNoCtx("sub-query cannot have +/-/phrase operator")
			}
		}

		for _, c := range p.Children {
			err := c.Validate()
			if err != nil {
				return err
			}
		}
	default:
		// LESSTHAN, GREATERTHAN, RANKLESS
		if len(p.Children) != 1 {
			return moerr.NewInternalErrorNoCtx("LESSTHAN, GREATERTHAN, RANKLESS must have single child only")
		}
		for _, c := range p.Children {
			if c.Operator != GROUP && c.Operator != TEXT && c.Operator != STAR {
				return moerr.NewInternalErrorNoCtx("double operator")
			}
		}

		for _, c := range p.Children {
			err := c.Validate()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func GetOp(op rune) int {
	switch op {
	case '+':
		return PLUS
	case '-':
		return MINUS
	case '<':
		return LESSTHAN
	case '>':
		return GREATERTHAN
	case '~':
		return RANKLESS
	/*
		case '*':
			return STAR
		case '"':
			return PHRASE
		case '(':
			return GROUP
	*/
	default:
		return TEXT
	}
}

func IsSubExpression(pattern string) bool {
	runeSlice := []rune(pattern)
	strlen := len(runeSlice)
	op := runeSlice[0]
	lastop := runeSlice[strlen-1]

	return (op == '(' && lastop == ')')
}

// first character is a operator
func CreatePattern(pattern string) (*Pattern, error) {
	if len(pattern) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("pattern is empty")
	}
	runeSlice := []rune(pattern)

	strlen := len(runeSlice)
	op := runeSlice[0]
	lastop := runeSlice[strlen-1]

	var operator int
	var word string
	if op == '(' && lastop == ')' {
		word = strings.TrimSpace(string(runeSlice[1 : strlen-1]))

		p, err := ParsePatternInBooleanMode(word)
		if err != nil {
			return nil, err
		}
		return &Pattern{Text: pattern, Operator: GROUP, Children: p}, nil

	}

	operator = GetOp(op)
	if operator == TEXT {
		if lastop == '*' {
			operator = STAR
		}
		return &Pattern{Text: pattern, Operator: operator}, nil
	}

	// check sub-expression
	word = string(runeSlice[1:])
	p, err := ParsePatternInBooleanMode(word)
	if err != nil {
		return nil, err
	}
	return &Pattern{Text: pattern, Operator: operator, Children: p}, nil
}

func ParsePhrase(pattern string) ([]*Pattern, error) {
	// phrase here
	offset := int32(0)
	isspace := false
	var children []*Pattern

	for pos, r := range pattern {
		if r == ' ' {
			if isspace {
				continue
			} else {
				children = append(children, &Pattern{Text: string(pattern[offset:pos]), Operator: TEXT, Position: offset})
			}
			isspace = true
		} else {
			if isspace {
				// start of the word
				offset = int32(pos)
			}
			isspace = false
		}
	}

	children = append(children, &Pattern{Text: string(pattern[offset:]), Operator: TEXT, Position: offset})
	return []*Pattern{{Text: pattern, Operator: PHRASE, Children: children}}, nil
}

// Parse the search string in boolean mode
func ParsePatternInBooleanMode(pattern string) ([]*Pattern, error) {

	if strings.HasPrefix(pattern, "\"") && strings.HasSuffix(pattern, "\"") {
		// phrase here
		ss := strings.Trim(pattern[1:len(pattern)-1], " ")
		if len(ss) == 0 {
			return nil, moerr.NewInternalErrorNoCtx("phrase is empty string")
		}

		return ParsePhrase(ss)
	}

	runeSlice := []rune(pattern)

	isspace := false
	offset := 0
	end := 0
	bracket := 0

	var tokens []*Pattern

	for i, r := range runeSlice {

		if bracket > 0 {
			if r == '(' {
				bracket += 1
				continue
			}

			if r != ')' {
				if i == len(runeSlice)-1 {
					return nil, moerr.NewInternalErrorNoCtx("no close bracket found")
				}
			} else {
				bracket -= 1
				if bracket == 0 {
					// found ()
					end = i
					p, err := CreatePattern(string(runeSlice[offset : end+1]))
					if err != nil {
						return nil, err
					}
					tokens = append(tokens, p)

					bracket = 0
					isspace = true
				}
			}
			continue
		}

		if !isspace {
			if r == ' ' {
				// something here
				isspace = true

				p, err := CreatePattern(string(runeSlice[offset : end+1]))
				if err != nil {
					return nil, err
				}
				tokens = append(tokens, p)
			} else if r == '(' {
				if i == len(runeSlice)-1 {
					return nil, moerr.NewInternalErrorNoCtx("no close bracket found")
				}

				bracket += 1
				end = i
			} else {
				end = i
				if i == len(runeSlice)-1 {
					p, err := CreatePattern(string(runeSlice[offset : end+1]))
					if err != nil {
						return nil, err
					}
					tokens = append(tokens, p)
				}
			}

			continue
		}

		// isspace == true && curent character is ' '
		if r == ' ' {
			// skipping space
			continue
		}

		// start of word
		isspace = false
		offset = i
		end = i

		if bracket == 0 {
			if r == '(' {
				if i == len(runeSlice)-1 {
					return nil, moerr.NewInternalErrorNoCtx("no close bracket found")
				}

				// open bracket found and find next close bracket
				bracket += 1
			}
		}

	}
	return tokens, nil
}

// Parse search string in natural language mode
func ParsePatternInNLMode(pattern string) ([]*Pattern, error) {
	runeSlice := []rune(pattern)
	ngram_size := 3
	// if number of character is small than Ngram size = 3, do prefix search
	if len(runeSlice) < ngram_size {
		return []*Pattern{{Text: pattern + "*", Operator: STAR}}, nil
	}

	list := make([]*Pattern, 0, 32)
	tok, _ := tokenizer.NewSimpleTokenizer([]byte(pattern))
	for t := range tok.Tokenize() {

		slen := t.TokenBytes[0]
		word := string(t.TokenBytes[1 : slen+1])

		list = append(list, &Pattern{Text: word, Operator: TEXT})
	}

	return list, nil
}

// Parse search string into list of patterns
func ParsePattern(pattern string, mode int64) ([]*Pattern, error) {
	switch mode {
	case int64(tree.FULLTEXT_NL), int64(tree.FULLTEXT_DEFAULT):
		// Natural Language Mode or default mode
		ps, err := ParsePatternInNLMode(pattern)
		if err != nil {
			return nil, err
		}
		return ps, nil
	case int64(tree.FULLTEXT_QUERY_EXPANSION), int64(tree.FULLTEXT_NL_QUERY_EXPANSION):
		return nil, moerr.NewInternalErrorNoCtx("Query Expansion mode not supported")
	case int64(tree.FULLTEXT_BOOLEAN):
		// BOOLEAN MODE

		lowerp := strings.ToLower(pattern)

		ps, err := ParsePatternInBooleanMode(lowerp)
		if err != nil {
			return nil, err
		}

		// Validate the patterns
		for _, p := range ps {
			err = p.Validate()
			if err != nil {
				return nil, err
			}
		}

		// re-order the pattern with the precedency PHRASE, PLUS > TEXT,STAR,GROUP,RANKLESS > MINUS
		// GROUP can only have LESSTHAN and GREATERTHAN children
		// PLUS, MINUS, RANKLESS can only have TEXT, STAR and GROUP Children and only have Single Child
		plus := findPatternByOperator(ps, PLUS)
		minus := findPatternByOperator(ps, MINUS)
		values := findValuePattern(ps)

		var finalp []*Pattern
		finalp = append(finalp, plus...)
		finalp = append(finalp, values...)
		finalp = append(finalp, minus...)

		return finalp, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid fulltext search mode")

	}
}
