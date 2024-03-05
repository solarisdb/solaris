// Copyright 2023 The acquirecloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package inmem

import (
	"context"
	"fmt"
	"github.com/gobwas/glob"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/kvs"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"sync"
	"time"
)

type (
	service struct {
		lock sync.Mutex

		recs      map[string]kvs.Record
		verChange map[string]*waiter
	}

	waiter struct {
		done    chan struct{}
		waiters int
	}

	keysIterator struct {
		res []string
	}
)

// New returns new kvs.Storage in memory
func New() kvs.Storage {
	res := new(service)
	res.recs = make(map[string]kvs.Record)
	res.verChange = make(map[string]*waiter)
	return res
}

func (s *service) Create(ctx context.Context, record kvs.Record) (string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	if r, ok := s.recs[record.Key]; ok {
		return r.Version, errors.ErrExist
	}
	record.Version = ulidutils.NewID()
	s.recs[record.Key] = record
	return record.Version, nil
}

func (s *service) Get(ctx context.Context, key string) (kvs.Record, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	r, ok := s.recs[key]
	if !ok {
		return kvs.Record{}, errors.ErrNotExist
	}
	if r.ExpiresAt != nil {
		if r.ExpiresAt.Before(time.Now()) {
			delete(s.recs, key)
			s.notifyWaiters(key)
			return kvs.Record{}, errors.ErrNotExist
		}
	}
	return r, nil
}

func (s *service) Put(ctx context.Context, record kvs.Record) (kvs.Record, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	record.Version = ulidutils.NewID()
	s.recs[record.Key] = record
	s.notifyWaiters(record.Key)
	return record, nil
}

func (s *service) PutMany(ctx context.Context, records []kvs.Record) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, r := range records {
		r.Version = ulidutils.NewID()
		s.recs[r.Key] = r
		s.notifyWaiters(r.Key)
	}
	return nil
}

func (s *service) GetMany(ctx context.Context, keys ...string) ([]*kvs.Record, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	res := make([]*kvs.Record, len(keys))
	for idx, key := range keys {
		r, ok := s.recs[key]
		if !ok {
			continue
		}
		if r.ExpiresAt != nil {
			if r.ExpiresAt.Before(time.Now()) {
				delete(s.recs, key)
				s.notifyWaiters(key)
				continue
			}
		}
		res[idx] = &r
	}
	return res, nil
}

func (s *service) CasByVersion(ctx context.Context, record kvs.Record) (kvs.Record, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	r, ok := s.recs[record.Key]
	if !ok {
		return kvs.Record{}, errors.ErrNotExist
	}
	if r.ExpiresAt != nil {
		if r.ExpiresAt.Before(time.Now()) {
			delete(s.recs, record.Key)
			s.notifyWaiters(record.Key)
			return kvs.Record{}, errors.ErrNotExist
		}
	}
	if r.Version != record.Version {
		return kvs.Record{}, errors.ErrConflict
	}
	record.Version = ulidutils.NewID()
	s.recs[record.Key] = record
	s.notifyWaiters(record.Key)
	return record, nil
}

func (s *service) Delete(ctx context.Context, key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if _, ok := s.recs[key]; !ok {
		return errors.ErrNotExist
	}
	delete(s.recs, key)
	s.notifyWaiters(key)
	return nil
}

func (s *service) WaitForVersionChange(ctx context.Context, key, ver string) error {
	for {
		s.lock.Lock()
		r, ok := s.recs[key]
		if !ok {
			s.lock.Unlock()
			return errors.ErrNotExist
		}
		if r.Version != ver {
			s.lock.Unlock()
			return nil
		}
		ws, ok := s.verChange[key]
		if !ok {
			ws = &waiter{done: make(chan struct{})}
			s.verChange[key] = ws
		}
		ws.waiters++
		s.lock.Unlock()

		select {
		case <-ctx.Done():
			s.lock.Lock()
			defer s.lock.Unlock()
			ws1, ok := s.verChange[key]
			if !ok || ws.done != ws1.done {
				return ctx.Err()
			}
			ws.waiters--
			if ws.waiters == 0 {
				close(ws.done)
				delete(s.verChange, key)
			}
			return ctx.Err()
		case <-ws.done:
			// need to check the version, go around
		}
	}
}

func (s *service) ListKeys(ctx context.Context, pattern string) (iterable.Iterator[string], error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	g, err := glob.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("could not compile the patter %q: %w", pattern, err)
	}
	res := []string{}
	for k := range s.recs {
		if g.Match(k) {
			res = append(res, k)
		}
	}
	return &keysIterator{res: res}, nil
}

func (s *service) notifyWaiters(key string) {
	ws, ok := s.verChange[key]
	if !ok {
		return
	}
	close(ws.done)
	delete(s.verChange, key)
}

var _ iterable.Iterator[string] = (*keysIterator)(nil)

func (k *keysIterator) HasNext() bool {
	return len(k.res) > 0
}

func (k *keysIterator) Next() (string, bool) {
	if !k.HasNext() {
		return "", false
	}
	res := k.res[0]
	k.res = k.res[1:]
	return res, true
}

func (k *keysIterator) Close() error {
	k.res = nil
	return nil
}
