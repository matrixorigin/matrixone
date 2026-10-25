// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package batch

import (
	"bytes"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
)

// Frozen encoder from source 3c7eb8de7f: the compatibility oracle deliberately
// does not call either new streaming implementation.
func legacyVectorBytes(v *vector.Vector) []byte {
	var w bytes.Buffer
	class := byte(vector.FLAT)
	if v.IsConst() {
		class = byte(vector.CONSTANT)
	}
	w.WriteByte(class)
	w.Write(types.EncodeType(v.GetType()))
	rows := uint32(v.Length())
	w.Write(types.EncodeUint32(&rows))
	length := uint32(v.GetType().TypeSize())
	if !v.IsConst() {
		length *= rows
	} else if v.IsConstNull() {
		length = 0
	}
	w.Write(types.EncodeUint32(&length))
	if length > 0 {
		w.Write(v.GetData()[:length])
	}
	area := uint32(len(v.GetArea()))
	w.Write(types.EncodeUint32(&area))
	w.Write(v.GetArea())
	nulls, err := v.GetNulls().Show()
	if err != nil {
		panic(err)
	}
	n := uint32(len(nulls))
	w.Write(types.EncodeUint32(&n))
	w.Write(nulls)
	sorted := v.GetSorted()
	w.Write(types.EncodeBool(&sorted))
	return w.Bytes()
}
func legacyBatchBytes(b *Batch) []byte {
	var w bytes.Buffer
	rows := int64(b.RowCount())
	w.Write(types.EncodeInt64(&rows))
	count := int32(len(b.Vecs))
	w.Write(types.EncodeInt32(&count))
	for _, v := range b.Vecs {
		p := legacyVectorBytes(v)
		size := uint32(len(p))
		w.Write(types.EncodeUint32(&size))
		w.Write(p)
	}
	count = int32(len(b.Attrs))
	w.Write(types.EncodeInt32(&count))
	for _, a := range b.Attrs {
		n := int32(len(a))
		w.Write(types.EncodeInt32(&n))
		w.WriteString(a)
	}
	_ = types.WriteSizeBytes(b.ExtraBuf, &w)
	w.Write(types.EncodeInt32(&b.Recursive))
	w.Write(types.EncodeInt32(&b.ShuffleIDX))
	return w.Bytes()
}

type shortCodecWriter struct{}

func (shortCodecWriter) Write(p []byte) (int, error) { return 0, nil }

type failingCodecWriter struct {
	left int
	err  error
}

func (w *failingCodecWriter) Write(p []byte) (int, error) {
	if w.left < len(p) {
		return 0, w.err
	}
	w.left -= len(p)
	return len(p), nil
}

func TestStreamCodecCompatibilityAndFailures(t *testing.T) {
	m := mpool.MustNew("stream-codec")
	defer mpool.DeleteMPool(m)
	b := New([]string{"id", "wide"})
	defer b.Clean(m)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixedList(b.Vecs[0], []int64{1, 2, 3}, nil, m))
	require.NoError(t, vector.AppendBytesList(b.Vecs[1], [][]byte{[]byte("inlined"), []byte(strings.Repeat("w", 131072)), nil}, []bool{false, false, true}, m))
	b.SetRowCount(3)
	b.ExtraBuf = []byte("extra")
	b.Recursive = 2
	b.ShuffleIDX = 9
	cases := []*Batch{b, New([]string{"constant", "null"})}
	defer cases[1].Clean(m)
	var err error
	cases[1].Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(strings.Repeat("c", 32)), 5, m)
	require.NoError(t, err)
	cases[1].Vecs[1] = vector.NewConstNull(types.T_int64.ToType(), 5, m)
	cases[1].SetRowCount(5)
	for _, bat := range cases {
		expected := legacyBatchBytes(bat)
		var actual bytes.Buffer
		require.NoError(t, bat.MarshalBinaryTo(&actual))
		require.Equal(t, expected, actual.Bytes())
		require.Equal(t, int64(len(expected)), bat.MarshalBinarySize())
		require.ErrorIs(t, bat.MarshalBinaryTo(shortCodecWriter{}), io.ErrShortWrite)
		boom := errors.New("write failed")
		require.ErrorIs(t, bat.MarshalBinaryTo(&failingCodecWriter{left: 64, err: boom}), boom)
	}
}
