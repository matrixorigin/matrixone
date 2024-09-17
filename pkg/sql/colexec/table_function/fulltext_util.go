package table_function

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/monlp/tokenizer"
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
*/

type Word struct {
	DocId    any
	Position []int64
	DocCount int32
}

type WordAccum struct {
	Id    int64
	Mode  int64
	Words map[any]*Word
}

type SearchAccum struct {
	TblName     string
	Mode        int64
	Pattern     []*Pattern
	Params      string
	WordAccums  map[string]*WordAccum
	SumDocCount map[string]int32
	Nrow        int64
}

func NewWordAccum(id int64, mode int64) *WordAccum {
	return &WordAccum{Id: id, Mode: mode, Words: make(map[any]*Word)}
}

func NewSearchAccum(tblname string, pattern string, mode int64, params string) (*SearchAccum, error) {

	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return nil, err
	}

	// re-order the pattern with the precedency PHRASE > PLUS > TEXT,STAR,GROUP,RANKLESS > MINUS
	// GROUP can only have LESSTHAN and GREATERTHAN children
	// PLUS, MINUS, RANKLESS can only have TEXT, STAR and GROUP Children and only have Single Child
	plus := findPatternByOperator(ps, PLUS)
	minus := findPatternByOperator(ps, MINUS)
	values := findValuePattern(ps)

	var finalp []*Pattern
	finalp = append(finalp, plus...)
	finalp = append(finalp, values...)
	finalp = append(finalp, minus...)

	return &SearchAccum{TblName: tblname, Mode: mode, Pattern: finalp, Params: params, WordAccums: make(map[string]*WordAccum)}, nil
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

func (s *SearchAccum) calculateDocCount() {
	sum_count := make(map[string]int32)

	for key := range s.WordAccums {
		acc := s.WordAccums[key]
		sum_count[key] = 0
		for doc_id := range acc.Words {
			sum_count[key] += acc.Words[doc_id].DocCount
		}
	}

	s.SumDocCount = sum_count
}

func (s *SearchAccum) PatternAnyPlus() bool {
	for _, p := range s.Pattern {
		if p.Operator == PLUS {
			return true
		}
	}
	return false
}

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

type Pattern struct {
	Text     string
	Operator int
	Children []*Pattern
}

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
		return result, nil
	}

	sum_doc_count := s.SumDocCount[key]

	if result == nil {
		result = make(map[any]float32)
	}

	for doc_id := range acc.Words {
		tf := float64(acc.Words[doc_id].DocCount)
		idf := math.Log10(float64(s.Nrow) / float64(sum_doc_count))
		tfidf := float32(tf * idf * idf)
		result[doc_id] = weight * tfidf
	}

	return result, nil
}

// Eval Plus Plus operation.  Basically AND operation between input argument and result from the previous Eval()
// e.g. (+ (text apple)) (+ (text banana))
func (p *Pattern) EvalPlusPlus(s *SearchAccum, arg, result map[any]float32) (map[any]float32, error) {

	if result == nil {
		return nil, nil
	}

	var keys []any
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
		return nil, nil
	}

	var keys []any
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

// Get the weight for compute the TFIDF
// LESSTHAN is lower the ranking
// GREATERTHAN is higher the ranking
// RANKLESS is to discourage the ranking but not delete such as Minus. weight is negative to discourage the ranking.
func (p *Pattern) GetWeight() float32 {
	if p.Operator == LESSTHAN {
		return float32(0.9)
	} else if p.Operator == GREATERTHAN {
		return float32(1.1)
	} else if p.Operator == RANKLESS {
		return float32(-1.0)
	}
	return float32(1.0)
}

// Eval() function to evaluate the previous result from Eval and the current pattern (with data from datasource)  and return map[doc_id]float32
func (p *Pattern) Eval(accum *SearchAccum, weight float32, result map[any]float32) (map[any]float32, error) {
	var err error

	nchild := len(p.Children)

	if nchild == 0 {
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

	} else if nchild == 1 {
		// PLUS, MINUS, LESSTHAN, GREATERTHAN, RANKLESS
		// get weight by type
		weight := p.GetWeight()

		if result == nil {
			// LESSTHAN, GREATERTHAN and RANKLESS
			return p.Children[0].Eval(accum, weight, nil)

		} else {
			child_result, err := p.Children[0].Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			// do PLUS (AND) and MINUS operation (REMOVE HASH) and OR operation
			if p.Operator == PLUS {
				// AND
				return p.EvalPlusPlus(accum, child_result, result)
			} else if p.Operator == MINUS {
				// MINUS
				return p.EvalMinus(accum, child_result, result)
			} else if p.Operator == GROUP || p.Operator == LESSTHAN || p.Operator == GREATERTHAN {
				return p.EvalOR(accum, child_result, result)
			} else {
				// OR
				if accum.PatternAnyPlus() {
					return p.EvalPlusOR(accum, child_result, result)
				} else {
					return p.EvalOR(accum, child_result, result)
				}
			}
		}
	} else {
		// GROUP, PHRASE

		if p.Operator == GROUP {
			// TODO: FIXME  use COMBINE instead of OR mode
			child_result := make(map[any]float32)
			for _, c := range p.Children {
				child_result, err = c.Eval(accum, weight, child_result)
				if err != nil {
					return nil, err
				}
			}

			return child_result, nil
		}

		if p.Operator == PHRASE {
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

			return result, nil
		}

	}

	return nil, moerr.NewInternalError(context.TODO(), "Eval() not handled")
}

// validate the Pattern
func (p *Pattern) Validate() error {
	if p.Operator == PLUS || p.Operator == MINUS {
		if len(p.Children) == 0 {
			return moerr.NewInternalError(context.TODO(), "+/- must have children with value")
		}
		for _, c := range p.Children {
			if c.Operator == PLUS || c.Operator == MINUS || c.Operator == PHRASE {
				return moerr.NewInternalError(context.TODO(), "double +/- operator")
			}
		}

		for _, c := range p.Children {
			err := c.Validate()
			if err != nil {
				return err
			}
		}

	} else if p.Operator == TEXT || p.Operator == STAR {
		if len(p.Children) > 0 {
			return moerr.NewInternalError(context.TODO(), "text Pattern cannot have children")
		}
	} else if p.Operator == PHRASE {
		for _, c := range p.Children {
			if c.Operator != TEXT {
				return moerr.NewInternalError(context.TODO(), "PHRASE can only have text Pattern")
			}
		}
	} else if p.Operator == GROUP {
		if len(p.Children) == 0 {
			return moerr.NewInternalError(context.TODO(), "sub-query is empty")
		}

		for _, c := range p.Children {
			if c.Operator == PLUS || c.Operator == MINUS || c.Operator == PHRASE {
				return moerr.NewInternalError(context.TODO(), "sub-query cannot have +/-/phrase operator")
			}
		}

		for _, c := range p.Children {
			err := c.Validate()
			if err != nil {
				return err
			}
		}

	} else {
		// LESSTHAN, GREATERTHAN, RANKLESS
		for _, c := range p.Children {
			if c.Operator != GROUP && c.Operator != TEXT && c.Operator != STAR {
				return moerr.NewInternalError(context.TODO(), "double operator")
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
		return nil, moerr.NewInternalError(context.TODO(), "pattern is empty")
	}
	runeSlice := []rune(pattern)

	strlen := len(runeSlice)
	op := runeSlice[0]
	lastop := runeSlice[strlen-1]

	var operator int
	var word string
	if op == '(' && lastop == ')' {
		operator = GROUP
		word = strings.TrimSpace(string(runeSlice[1 : strlen-1]))

		p, err := ParsePatternInBooleanMode(word)
		if err != nil {
			return nil, err
		}
		return &Pattern{Text: pattern, Operator: GROUP, Children: p}, nil

	}

	word = string(runeSlice[1:])
	operator = GetOp(op)
	if operator == TEXT {
		if lastop == '*' {
			operator = STAR
		}
		word = string(runeSlice)
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

func ParsePatternInBooleanMode(pattern string) ([]*Pattern, error) {

	if strings.HasPrefix(pattern, "\"") && strings.HasSuffix(pattern, "\"") {
		// phrase here
		ss := strings.Split(pattern[1:len(pattern)-1], " ")
		var children []*Pattern

		for _, s := range ss {
			children = append(children, &Pattern{Text: s, Operator: TEXT})
		}

		return []*Pattern{&Pattern{Text: pattern, Operator: PHRASE, Children: children}}, nil
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
					return nil, moerr.NewInternalError(context.TODO(), "no close bracket found")
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

		if isspace == false {
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
					return nil, moerr.NewInternalError(context.TODO(), "no close bracket found")
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
					return nil, moerr.NewInternalError(context.TODO(), "no close bracket found")
				}

				// open bracket found and find next close bracket
				bracket += 1
			}
		}

	}
	return tokens, nil
}

func ParsePatternInNLMode(pattern string) ([]*Pattern, error) {
	runeSlice := []rune(pattern)
	ngram_size := 3
	// if number of character is small than Ngram size = 3, do prefix search
	if len(runeSlice) < ngram_size {
		return []*Pattern{&Pattern{Text: pattern + "*", Operator: STAR}}, nil
	}

	var list []*Pattern
	tok, _ := tokenizer.NewSimpleTokenizer([]byte(pattern))
	for t := range tok.Tokenize() {

		slen := t.TokenBytes[0]
		word := string(t.TokenBytes[1 : slen+1])

		list = append(list, &Pattern{Text: word, Operator: TEXT})
	}

	return list, nil
}

func ParsePattern(pattern string, mode int64) ([]*Pattern, error) {
	if mode == int64(tree.FULLTEXT_NL) || mode == int64(tree.FULLTEXT_DEFAULT) {
		// Natural Language Mode or default mode
		ps, err := ParsePatternInNLMode(pattern)
		if err != nil {
			return nil, err
		}
		return ps, nil
	} else if mode == int64(tree.FULLTEXT_QUERY_EXPANSION) || mode == int64(tree.FULLTEXT_NL_QUERY_EXPANSION) {
		return nil, moerr.NewInternalError(context.TODO(), "Query Expansion mode not supported")
	} else if mode == int64(tree.FULLTEXT_BOOLEAN) {
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
		return ps, nil
	}

	return nil, moerr.NewInternalError(context.TODO(), "invalid fulltext search mode")
}
