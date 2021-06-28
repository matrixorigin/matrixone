package parser

type lexState int

type commentState int


const (

)

//input string stream
type stringStream struct {
	s string
	p int
	tokStart int
	tokEnd int
	nextState lexState
	semicolonPos int
	inComment commentState
	inCommentSaved commentState
}

func (ss *stringStream) assertPosInString(pos int){
	if pos > len(ss.s) {
		panic("pos >= len(s)")
	}
}

func (ss *stringStream) skipBinary(n int)  {
	ss.assertPosInString(n + ss.p)
	ss.p += n
}

func (ss *stringStream) next() byte {
	ss.assertPosInString(ss.p+1)

	return 0
}

type lexer struct {
	yyLexer
}


