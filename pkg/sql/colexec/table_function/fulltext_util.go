package table_function

import (
	"context"
	"fmt"
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

type FullTextBooleanOperator int

var (
	NoOp        = 0
	Plus        = 1
	Minus       = 2
	LessThan    = 3
	GreaterThan = 4
	Group       = 5
	RankLess    = 6
	Star        = 7
	Phrase      = 8
)

type Pattern struct {
	Text     string
	Operator int
	Children []*Pattern
}

func (p *Pattern) String() string {
	str := fmt.Sprintf("[Text:%s, Operator:%d, Children:[", p.Text, p.Operator)
	for _, c := range p.Children {
		str += c.String()
		str += ", "
	}
	str += "]]"
	return str
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
	fmt.Printf("PATTERN %s\n", pattern)

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
	//fmt.Printf("OP: %c, Operator: %d, Word: *%s*\n", op, operator, word)
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

	//fmt.Printf("len = %d\n", len(runeSlice))

	isspace := false
	offset := 0
	end := 0
	bracket := false

	var tokens []*Pattern

	for i, r := range runeSlice {

		if bracket {
			if r != ')' {
				if i == len(runeSlice)-1 {
					return nil, moerr.NewInternalError(context.TODO(), "no close bracket found")
				}
				continue
			} else {
				// found ()
				end = i
				//fmt.Printf("bracket (%d, %d)", offset, end)
				//tokens = append(tokens, string(runeSlice[offset:end+1]))
				p, err := CreatePattern(string(runeSlice[offset : end+1]))
				if err != nil {
					return nil, err
				}
				tokens = append(tokens, p)

				bracket = false
				isspace = true
				continue
			}
		}

		if i == len(runeSlice)-1 {
			if isspace == false {
				end = i
				//fmt.Printf("word (%d, %d), ", offset, end)
				//tokens = append(tokens, string(runeSlice[offset:end+1]))
				p, err := CreatePattern(string(runeSlice[offset : end+1]))
				if err != nil {
					return nil, err
				}
				tokens = append(tokens, p)
			}
			continue
		}

		if isspace == false {
			if r == ' ' {
				// something here
				isspace = true

				//fmt.Printf("word (%d, %d), ", offset, end)
				//tokens = append(tokens, string(runeSlice[offset:end+1]))
				p, err := CreatePattern(string(runeSlice[offset : end+1]))
				if err != nil {
					return nil, err
				}
				tokens = append(tokens, p)
			} else if r == '(' {
				bracket = true
				end = i
			} else {
				end = i
			}

			continue
		}
		if isspace && r == ' ' {
			// skipping space
			continue
		}

		// start of word
		isspace = false
		offset = i
		end = i

		if bracket == false {
			if r == '(' {
				// open bracket found and find next close bracket
				bracket = true
			}
		}

	}
	return tokens, nil
}
