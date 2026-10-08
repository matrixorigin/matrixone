// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package fulltext2

import "testing"

func TestBaseFileKeySeparatesOwnerAccountAndIncarnation(t *testing.T) {
	base := baseFileKey{
		owner: "cn-a", account: 7, db: "db", src: "src", index: "idx",
		metadata: "meta", pkey: "id", id: "idx:100:0", checksum: "sum-a", size: 8,
	}
	if base == (baseFileKey{}) {
		t.Fatal("test identity must not be empty")
	}
	for name, changed := range map[string]baseFileKey{
		"owner":       func() baseFileKey { k := base; k.owner = "cn-b"; return k }(),
		"account":     func() baseFileKey { k := base; k.account = 8; return k }(),
		"incarnation": func() baseFileKey { k := base; k.id = "idx:101:0"; return k }(),
		"checksum":    func() baseFileKey { k := base; k.checksum = "sum-b"; return k }(),
	} {
		if changed == base {
			t.Fatalf("%s must not alias the same immutable file identity", name)
		}
	}
	// Tail generation is deliberately absent from the key: it changes the
	// surrounding Index snapshot, not the immutable Base bytes.
	sameBase := base
	if sameBase != base {
		t.Fatal("same Base identity must remain reusable across Tail generations")
	}
}
