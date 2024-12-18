// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

package buntdbstore_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/creachadair/buntdbstore"
	"github.com/creachadair/ffs/blob/storetest"
)

var keepOutput = flag.Bool("keep", false, "Keep test output after running")

func TestStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "buntdbtest")
	if err != nil {
		t.Fatalf("Creating temp directory: %v", err)
	}
	path := filepath.Join(dir, "bunt.db")
	t.Logf("Test store: %s", path)
	if !*keepOutput {
		defer os.RemoveAll(dir) // best effort cleanup
	}

	s, err := buntdbstore.Open(path, nil)
	if err != nil {
		t.Fatalf("Creating store at %q: %v", path, err)
	}
	storetest.Run(t, s)
}
