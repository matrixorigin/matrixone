package table_function

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

	// TODO: tokenize the pattern based on mode and params
	// use space as separator for now
	if mode != 0 {
		return nil, moerr.NewNotSupported(context.TODO(), "mode not supported")
	}

	ps, err := ParsePattern(pattern, mode)
	if err != nil {
		return nil, err
	}

	/*
		pattern = strings.ToLower(pattern)
		ps, err := ParsePatternInBooleanMode(pattern)
		if err != nil {
			return nil, err
		}
	*/

	// TODO: re-arrange the pattern with the precedency Phrase > Plus > NoOp,Star,Group,RankLess > Minus
	// Group can only have LessThan and GreaterThan children
	// Plus, Minus, RankLess can only have NoOp, Star and Group Children and only have Single Child

	// TODO: Validate the patterns

	return &SearchAccum{TblName: tblname, Mode: mode, Pattern: ps, Params: params, WordAccums: make(map[string]*WordAccum)}, nil
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
		if p.Operator == Plus {
			return true
		}
	}
	return false
}

type FullTextBooleanOperator int

var (
	NoOp        = 0
	Star        = 1
	Plus        = 2
	Minus       = 3
	LessThan    = 4
	GreaterThan = 5
	RankLess    = 6
	Group       = 7
	Phrase      = 8
)

func OperatorToString(op int) string {
	switch op {
	case NoOp:
		return "text"
	case Star:
		return "*"
	case Plus:
		return "+"
	case Minus:
		return "-"
	case LessThan:
		return "<"
	case GreaterThan:
		return ">"
	case RankLess:
		return "~"
	case Group:
		return "group"
	case Phrase:
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
	if p.Operator == NoOp || p.Operator == Star {
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

func (p *Pattern) Eval(accum *SearchAccum, weight float32, result map[any]float32) (map[any]float32, error) {
	var err error

	nchild := len(p.Children)
	// create, init a result map and pattern must not be Minus Type
	if result == nil && p.Operator == Minus {
		return nil, moerr.NewInternalError(context.TODO(), "Pattern cannot be inited with '-' operator")
	}

	if nchild == 0 {
		// leaf node: NoOp, Star
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
		// Plus, Minus, LessThan, GreaterThan, RankLess
		// TODO: set weight by type
		weight := float32(1.0)

		if result == nil {
			// LessThan, GreaterThan and RankLess
			return p.Children[0].Eval(accum, weight, nil)

		} else {
			child_result, err := p.Children[0].Eval(accum, weight, nil)
			if err != nil {
				return nil, err
			}

			// do Plus (AND) and Minus operation (REMOVE HASH) and OR operation
			if p.Operator == Plus {
				// AND
				return p.EvalPlusPlus(accum, child_result, result)
			} else if p.Operator == Minus {
				// MINUS
				return p.EvalMinus(accum, child_result, result)
			} else if p.Operator == Group || p.Operator == LessThan || p.Operator == GreaterThan {
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
		// Group, Phrase

		if p.Operator == Group {
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

		if p.Operator == Phrase {
			// all children are NoOp and AND operations
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

func GetOp(op rune) int {
	switch op {
	case '+':
		return Plus
	case '-':
		return Minus
	case '<':
		return LessThan
	case '>':
		return GreaterThan
	case '~':
		return RankLess
	/*
		case '*':
			return Star
		case '"':
			return Phrase
		case '(':
			return Group
	*/
	default:
		return NoOp
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
		operator = Group
		word = strings.TrimSpace(string(runeSlice[1 : strlen-1]))

		p, err := ParsePatternInBooleanMode(word)
		if err != nil {
			return nil, err
		}
		return &Pattern{Text: pattern, Operator: Group, Children: p}, nil

	}

	word = string(runeSlice[1:])
	operator = GetOp(op)
	if operator == NoOp {
		if lastop == '*' {
			operator = Star
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
			children = append(children, &Pattern{Text: s, Operator: NoOp})
		}

		return []*Pattern{&Pattern{Text: pattern, Operator: Phrase, Children: children}}, nil
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

func (p *Pattern) Valid() error {
	if p.Operator == Plus || p.Operator == Minus {
		if len(p.Children) == 0 {
			return moerr.NewInternalError(context.TODO(), "+/- must have children with value")
		}
		for _, c := range p.Children {
			if c.Operator == Plus || c.Operator == Minus || c.Operator == Phrase {
				return moerr.NewInternalError(context.TODO(), "double +/- operator")
			}
		}

		for _, c := range p.Children {
			err := c.Valid()
			if err != nil {
				return err
			}
		}

	} else if p.Operator == NoOp || p.Operator == Star {
		if len(p.Children) > 0 {
			return moerr.NewInternalError(context.TODO(), "text Pattern cannot have children")
		}
	} else if p.Operator == Phrase {
		for _, c := range p.Children {
			if c.Operator != NoOp {
				return moerr.NewInternalError(context.TODO(), "Phrase can only have text Pattern")
			}
		}
	} else if p.Operator == Group {
		if len(p.Children) == 0 {
			return moerr.NewInternalError(context.TODO(), "sub-query is empty")
		}

		for _, c := range p.Children {
			if c.Operator == Plus || c.Operator == Minus || c.Operator == Phrase {
				return moerr.NewInternalError(context.TODO(), "sub-query cannot have +/-/phrase operator")
			}
		}

		for _, c := range p.Children {
			err := c.Valid()
			if err != nil {
				return err
			}
		}

	} else {
		// LessThan, GreaterThan, RankLess
		for _, c := range p.Children {
			if c.Operator != Group && c.Operator != NoOp && c.Operator != Star {
				return moerr.NewInternalError(context.TODO(), "double operator")
			}
		}

		for _, c := range p.Children {
			err := c.Valid()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func ParsePattern(pattern string, mode int64) ([]*Pattern, error) {
	if mode != 0 {
		return nil, moerr.NewInternalError(context.TODO(), "only support default mode 0")
	}

	lowerp := strings.ToLower(pattern)

	ps, err := ParsePatternInBooleanMode(lowerp)
	if err != nil {
		return nil, err
	}

	// TODO: Validate the patterns
	for _, p := range ps {
		err = p.Valid()
		if err != nil {
			return nil, err
		}
	}

	// TODO: re-arrange the pattern with the precedency Phrase > Plus > NoOp,Star,Group,RankLess > Minus
	// Group can only have LessThan and GreaterThan children
	// Plus, Minus, RankLess can only have NoOp, Star and Group Children and only have Single Child

	return ps, nil
}
