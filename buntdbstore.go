// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package buntdbstore implements the [blob.Store] interfaace using [buntdb].
//
// [buntdb]: https://github.com/tidwall/buntdb
package buntdbstore

import (
	"context"
	"errors"

	"github.com/creachadair/ffs/blob"
	"github.com/tidwall/buntdb"
)

// Store implements the [blob.Store] interface using a buntdb database.
type Store struct {
	db *buntdb.DB
}

// Opener constructs a [Store] from an address comprising a path.
func Opener(_ context.Context, addr string) (blob.Store, error) { return Open(addr, nil) }

// Open creates a [Store] by opening the buntdb database at path.
func Open(path string, opts *Options) (*Store, error) {
	db, err := buntdb.Open(path)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Get implements part of the [blob.Store] interface.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
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

// Put implements part of the [blob.Store] interface.
func (s *Store) Put(ctx context.Context, opts blob.PutOptions) error {
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

// Delete implements part of the [blob.Store] interface.
func (s *Store) Delete(ctx context.Context, key string) error {
	return s.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		if errors.Is(err, buntdb.ErrNotFound) {
			return blob.KeyNotFound(key)
		}
		return err
	})
}

// List implements part of the [blob.Store] interface.
func (s *Store) List(ctx context.Context, start string, f func(string) error) error {
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

// Len implements part of the [blob.Store] interface.
func (s *Store) Len(ctx context.Context) (n int64, err error) {
	err = s.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendKeys("*", func(_, _ string) bool {
			n++
			return true
		})
	})
	return
}

// Close implements part of the [blob.Store] interface.
func (s *Store) Close(_ context.Context) error {
	merr := s.db.Shrink()
	if errors.Is(merr, buntdb.ErrDatabaseClosed) {
		return nil
	}
	return errors.Join(merr, s.db.Close())
}

// Options provide options for opening a buntdb database.
// A nil *Options is ready for use and provides default values.
type Options struct{}
