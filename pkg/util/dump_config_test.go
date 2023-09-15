package util

import (
    "github.com/stretchr/testify/assert"
    "testing"
)

type abc struct {
    A string
    B string
}

type kkk struct {
    kk string
    jj string
}

type def struct {
    DDD []kkk
    ABC []abc `toml:"abc"`
    EEE [3]abc
    MMM map[string]string
    NNN map[string]abc
    PPP *abc
    QQQ *abc
    RRR map[string]string
}

func Test_flatten(t *testing.T) {
    cf1 := def{
        ABC: []abc{
            {"a", "a1"},
            {"b", "b1"},
        },
        EEE: [3]abc{
            {"a", "a1"},
            {"b", "b1"},
            {"c", "c1"},
        },
        MMM: map[string]string{
            "abc":  "ABC",
            "abcd": "ABCd",
        },
        NNN: map[string]abc{
            "abc":  {"a", "a1"},
            "abcd": {"a", "a1"},
        },
        PPP: &abc{"a", "a1"},
    }
    exp := newExporter()
    err := flatten(cf1, "", "", exp)
    assert.NoError(t, err)
    err = flatten(&cf1, "", "", exp)
    assert.NoError(t, err)
}
