package event

type listensers struct {
	ls []Listener
}

func NewListensers(l1, l2 Listener) *listensers {
	s := &listensers{
		ls: make([]Listener, 0, 2),
	}
	s.ls = append(s.ls, l1, l2)
	return s
}

func (s *listensers) OnPreSplit(event *SplitEvent) error {
	var err error
	for _, l := range s.ls {
		if err = l.OnPreSplit(event); err != nil {
			break
		}
	}
	return err
}

func (s *listensers) OnPostSplit(res error, event *SplitEvent) error {
	var err error
	for _, l := range s.ls {
		if err = l.OnPostSplit(res, event); err != nil {
			break
		}
	}
	return err
}

func (s *listensers) OnBackgroundError(err error) error {
	var ret error
	for _, l := range s.ls {
		if ret = l.OnBackgroundError(err); ret != nil {
			break
		}
	}
	return err
}
