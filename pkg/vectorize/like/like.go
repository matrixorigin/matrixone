package like

/*
var (
	sLike func(*vector.Bytes, []byte, []int64) ([]int64, error)
)

func sLikePure(s *vector.Bytes, expr []byte) ([]int64, error) {
	n := uint32(len(expr))
	rs := sel.Pool.Get().([]int64)
	rs = rs[:0]
	if n == 0 {
		for i, m := range s.Ns {
			if m == 0 {
				rs = append(rs, int64(i))
			}
		}
		return rs, nil
	}
	if n == 1 && expr[0] == '%' {
		for i := range s.Ns {
			rs = append(rs, int64(i))
		}
		return rs, nil
	}
	if n == 1 && expr[0] == '_' {
		for i, m := range s.Ns {
			if m == 1 {
				rs = append(rs, int64(i))
			}
		}
		return rs, nil
	}
	if n > 1 && !bytes.ContainsAny(expr[1:len(expr)-1], "_%") {
		c0 := expr[0]   // first character
		c1 := expr[n-1] // last character
		switch {
		case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
			for i, o := range s.Os {
				if s.Ns[i] == n && bytes.Compare(expr, s.Data[o:o+s.Ns[i]]) == 0 {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		case c0 == '_' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			for i, o := range s.Os {
				if s.Ns[i] == n && bytes.Compare(suffix, s.Data[o+1:o+s.Ns[i]]) == 0 {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		case c0 == '%' && !(c1 == '%' || c1 == '_'):
			suffix := expr[1:]
			for i, o := range s.Os {
				if bytes.HasSuffix(s.Data[o:o+s.Ns[i]], suffix) {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		case c1 == '_' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			for i, o := range s.Os {
				if s.Ns[i] == n && bytes.Compare(prefix, s.Data[o:o+s.Ns[i]-1]) == 0 {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		case c1 == '%' && !(c0 == '%' || c0 == '_'):
			prefix := expr[:n-1]
			for i, o := range s.Os {
				if s.Ns[i] >= n && bytes.Compare(s.Data[o:o+uint32(n-1)], prefix) == 0 {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		case c0 == '%' && c1 == '%':
			substr := expr[1 : n-1]
			for i, o := range s.Os {
				if bytes.Contains(s.Data[o:o+s.Ns[i]], substr) {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		case c0 == '%' && c1 == '_':
			suffix := expr[1 : n-1]
			for i, o := range s.Os {
				if s.Ns[i] > 0 && bytes.HasSuffix(s.Data[o:o+s.Ns[i]-1], suffix) {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		case c0 == '_' && c1 == '%':
			prefix := expr[1 : n-1]
			for i, o := range s.Os {
				if s.Ns[i] > 0 && bytes.HasPrefix(s.Data[o+1:o+s.Ns[i]], prefix) {
					rs = append(rs, int64(i))
				}
			}
			return rs, nil
		}
	}
	reg, err := regexp.Compile(convert(expr))
	if err != nil {
		return nil, err
	}
	for i, o := range s.Os {
		if reg.Match(s.Data[o : o+s.Ns[i]]) {
			rs = append(rs, int64(i))
		}
	}
	return rs, nil
}

func convert(expr []byte) string {
	return fmt.Sprintf("^(?s:%s)$", replace(*(*string)(unsafe.Pointer(&expr))))
}

func replace(s string) string {
	var oc rune

	r := make([]byte, len(s)+strings.Count(s, `%`))
	w := 0
	start := 0
	for len(s) > start {
		c, wid := utf8.DecodeRuneInString(s[start:])
		if oc == '\\' {
			w += copy(r[w:], s[start:start+wid])
			start += wid
			oc = 0
			continue
		}
		switch c {
		case '_':
			w += copy(r[w:], []byte{'*'})
		case '%':
			w += copy(r[w:], []byte{'.', '*'})
		case '\\':
		default:
			w += copy(r[w:], s[start:start+wid])
		}
		start += wid
		oc = c
	}
	return string(r)
}

func isWildcard(c byte) bool {
	return c == '%' || c == '_'
}
*/
