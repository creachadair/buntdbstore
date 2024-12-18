// Copyright (C) 2024 Michael J. Fromberger. All Rights Reserved.

// Package buntdbstore implements the [blob.StoreCloser] interface on [buntdb].
//
// [buntdb]: https://github.com/tidwall/buntdb
package buntdbstore

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/creachadair/ffs/blob"
	"github.com/creachadair/ffs/storage/dbkey"
	"github.com/tidwall/buntdb"
)

// Store implements the [blob.StoreCloser] interface using a BuntDB instance.
type Store struct {
	*dbMonitor
}

type dbMonitor struct {
	DB     *buntdb.DB
	prefix dbkey.Prefix

	μ    sync.Mutex
	subs map[string]*dbMonitor
	kvs  map[string]KV
}

// Keyspace implements part of the [blob.Store] interface.
// A successful result has concrete type [KV].
// This implementation never reports an error.
func (d *dbMonitor) Keyspace(ctx context.Context, name string) (blob.KV, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	kv, ok := d.kvs[name]
	if !ok {
		kv = KV{db: d.DB, prefix: d.prefix.Keyspace(name)}
		d.kvs[name] = kv
	}
	return kv, nil
}

// Sub implements part of the [blob.Store] interface.
// This implementation never reports an error.
func (d *dbMonitor) Sub(ctx context.Context, name string) (blob.Store, error) {
	d.μ.Lock()
	defer d.μ.Unlock()

	sub, ok := d.subs[name]
	if !ok {
		sub = &dbMonitor{
			DB:     d.DB,
			prefix: d.prefix.Sub(name),
			subs:   make(map[string]*dbMonitor),
			kvs:    make(map[string]KV),
		}
		d.subs[name] = sub
	}
	return sub, nil
}

// Close implements part of the [blob.StoreCloser] interface.
func (s Store) Close(_ context.Context) error {
	merr := s.DB.Shrink()
	if errors.Is(merr, buntdb.ErrDatabaseClosed) {
		return nil
	}
	return errors.Join(merr, s.DB.Close())
}

// Opener constructs a [KV] from an address comprising a path.
func Opener(_ context.Context, addr string) (blob.StoreCloser, error) { return Open(addr, nil) }

// Open creates a [KV] by opening the buntdb database at path.
func Open(path string, opts *Options) (blob.StoreCloser, error) {
	db, err := buntdb.Open(path)
	if err != nil {
		return nil, err
	}
	return Store{dbMonitor: &dbMonitor{
		DB:   db,
		subs: make(map[string]*dbMonitor),
		kvs:  make(map[string]KV),
	}}, nil
}

// KV implements the [blob.KV] interface using a buntdb database.
type KV struct {
	db     *buntdb.DB
	prefix dbkey.Prefix
}

// Get implements part of the [blob.KV] interface.
func (s KV) Get(ctx context.Context, key string) ([]byte, error) {
	var data string
	err := s.db.View(func(tx *buntdb.Tx) error {
		var gerr error
		data, gerr = tx.Get(s.prefix.Add(key))
		return gerr
	})
	if errors.Is(err, buntdb.ErrNotFound) {
		return nil, blob.KeyNotFound(key)
	}
	return []byte(data), err
}

// Put implements part of the [blob.KV] interface.
func (s KV) Put(ctx context.Context, opts blob.PutOptions) error {
	return s.db.Update(func(tx *buntdb.Tx) error {
		key := s.prefix.Add(opts.Key)
		if !opts.Replace {
			if _, err := tx.Get(key); err == nil {
				return blob.KeyExists(opts.Key)
			}
		}
		_, _, err := tx.Set(key, string(opts.Data), nil)
		return err
	})
}

// Delete implements part of the [blob.KV] interface.
func (s KV) Delete(ctx context.Context, key string) error {
	return s.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(s.prefix.Add(key))
		if errors.Is(err, buntdb.ErrNotFound) {
			return blob.KeyNotFound(key)
		}
		return err
	})
}

// List implements part of the [blob.KV] interface.
func (s KV) List(ctx context.Context, start string, f func(string) error) error {
	return s.db.View(func(tx *buntdb.Tx) error {
		var ferr error
		if err := tx.AscendGreaterOrEqual("", s.prefix.Add(start), func(key, _ string) bool {
			if !strings.HasPrefix(key, string(s.prefix)) {
				return false // no more belonging to this keyspace
			}
			ferr = f(s.prefix.Remove(key))
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
func (s KV) Len(ctx context.Context) (n int64, err error) {
	err = s.db.View(func(tx *buntdb.Tx) error {
		return tx.AscendGreaterOrEqual("", string(s.prefix), func(key, _ string) bool {
			if !strings.HasPrefix(key, string(s.prefix)) {
				return false // no more belonging to this keyspace
			}
			n++
			return true
		})
	})
	return
}

// Options provide options for opening a buntdb database.
// A nil *Options is ready for use and provides default values.
type Options struct{}
