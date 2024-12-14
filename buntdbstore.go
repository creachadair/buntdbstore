// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package buntdbstore implements the [blob.KV] interfaace using [buntdb].
//
// [buntdb]: https://github.com/tidwall/buntdb
package buntdbstore

import (
	"context"
	"errors"

	"github.com/creachadair/ffs/blob"
	"github.com/tidwall/buntdb"
)

// KV implements the [blob.KV] interface using a buntdb database.
type KV struct {
	db *buntdb.DB
}

// Opener constructs a [KV] from an address comprising a path.
func Opener(_ context.Context, addr string) (blob.KV, error) { return Open(addr, nil) }

// Open creates a [KV] by opening the buntdb database at path.
func Open(path string, opts *Options) (*KV, error) {
	db, err := buntdb.Open(path)
	if err != nil {
		return nil, err
	}
	return &KV{db: db}, nil
}

// Get implements part of the [blob.KV] interface.
func (s *KV) Get(ctx context.Context, key string) ([]byte, error) {
	var data string
	err := s.db.View(func(tx *buntdb.Tx) error {
		var gerr error
		data, gerr = tx.Get(key)
		return gerr
	})
	if errors.Is(err, buntdb.ErrNotFound) {
		return nil, blob.KeyNotFound(key)
	}
	return []byte(data), err
}

// Put implements part of the [blob.KV] interface.
func (s *KV) Put(ctx context.Context, opts blob.PutOptions) error {
	return s.db.Update(func(tx *buntdb.Tx) error {
		if !opts.Replace {
			if _, err := tx.Get(opts.Key); err == nil {
				return blob.KeyExists(opts.Key)
			}
		}
		_, _, err := tx.Set(opts.Key, string(opts.Data), nil)
		return err
	})
}

// Delete implements part of the [blob.KV] interface.
func (s *KV) Delete(ctx context.Context, key string) error {
	return s.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		if errors.Is(err, buntdb.ErrNotFound) {
			return blob.KeyNotFound(key)
		}
		return err
	})
}

// List implements part of the [blob.KV] interface.
func (s *KV) List(ctx context.Context, start string, f func(string) error) error {
	return s.db.View(func(tx *buntdb.Tx) error {
		var ferr error
		if err := tx.AscendGreaterOrEqual("", start, func(key, _ string) bool {
			ferr = f(key)
			return ferr == nil
		}); err != nil {
			return err
		}
		if errors.Is(ferr, blob.ErrStopListing) {
			return nil
		}
		return ferr
	})
}

// Len implements part of the [blob.KV] interface.
func (s *KV) Len(ctx context.Context) (n int64, err error) {
	err = s.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys("*", func(_, _ string) bool {
			n++
			return true
		})
	})
	return
}

// Close implements part of the [blob.KV] interface.
func (s *KV) Close(_ context.Context) error {
	merr := s.db.Shrink()
	if errors.Is(merr, buntdb.ErrDatabaseClosed) {
		return nil
	}
	return errors.Join(merr, s.db.Close())
}

// Options provide options for opening a buntdb database.
// A nil *Options is ready for use and provides default values.
type Options struct{}
