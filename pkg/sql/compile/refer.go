package compile

func IncRef(s *Scope, attrs []string) {
	for _, attr := range attrs {
		if ref, ok := s.Proc.Refer[attr]; ok {
			s.Proc.Refer[attr] = ref + 1
			continue
		}
		switch s.Magic {
		case Normal:
			s.Data.Refs[attr]++
		case Merge:
			for i := range s.Ss {
				IncRef(s.Ss[i], attrs)
			}
		}
	}
}
