package bytejson

import "testing"
import "github.com/stretchr/testify/require"

// A string scan gets $."a.b" wrong; the canonical parse must not.
func TestTerminalKey(t *testing.T) {
	for _, tc := range []struct {
		path, key string
		ok        bool
	}{
		{"$.foo", "foo", true},
		{"$.a.b.c", "c", true},
		{`$."a.b"`, "a.b", true}, // the quoted-key case a string split breaks
		{`$.x."a.b"`, "a.b", true},
		{"$.a[0]", "a", true}, // trailing subscript keeps the enclosing key
		{"$.a[0].b", "b", true},
		{"$.a[0][1]", "a", true},
		{"$", "", false},
		{"$[0]", "", false},
		{"$.a[*]", "", false}, // non-deterministic
		{"$.a.*", "", false},
		{"$**.b", "", false},
	} {
		p, err := ParseJsonPath(tc.path)
		if err != nil {
			require.False(t, tc.ok, "%s: parse error %v", tc.path, err)
			continue
		}
		k, ok := p.TerminalKey()
		require.Equal(t, tc.ok, ok, tc.path)
		if tc.ok {
			require.Equal(t, tc.key, k, tc.path)
		}
	}
}
