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
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/kvs"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
	"time"
)

func TestService_Create(t *testing.T) {
	s := New()
	r := kvs.Record{Key: "aa"}
	v, err := s.Create(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, v, r.Version)

	_, err = s.Create(context.Background(), r)
	assert.Equal(t, errors.ErrExist, err)
}

func TestService_Get(t *testing.T) {
	s := New()
	r := kvs.Record{Key: "aa"}

	_, err := s.Get(nil, r.Key)
	assert.Equal(t, errors.ErrNotExist, err)
	v, err := s.Create(context.Background(), r)
	assert.Nil(t, err)

	r1, err := s.Get(nil, r.Key)
	assert.Nil(t, err)
	assert.Equal(t, v, r1.Version)

	r.Key = "ddd"
	r.ExpiresAt = cast.Ptr(time.Now().Add(-time.Millisecond))
	_, err = s.Create(context.Background(), r)
	assert.Nil(t, err)
	_, err = s.Get(nil, r.Key)
	assert.Equal(t, errors.ErrNotExist, err)
}

func TestService_Put(t *testing.T) {
	s := New()
	r := kvs.Record{Key: "aa"}
	r1, err := s.Put(nil, r)
	assert.Nil(t, err)
	assert.NotEqual(t, r1.Version, r.Version)
	r.Version = r1.Version
	assert.Equal(t, r1, r)

	r, err = s.Get(nil, r1.Key)
	assert.Nil(t, err)
	assert.Equal(t, r1, r)

	r.Value = []byte("ddd")
	r1, err = s.Put(nil, r)
	assert.Nil(t, err)
	assert.NotEqual(t, r1.Version, r.Version)
	r.Version = r1.Version
	assert.Equal(t, r1, r)

	r, err = s.Get(nil, r1.Key)
	assert.Nil(t, err)
	assert.Equal(t, r1, r)
}

func TestService_PutMany(t *testing.T) {
	s := New()
	err := s.PutMany(nil, []kvs.Record{{Key: "aa", Value: []byte("aa1")}, {Key: "bb", Value: []byte("bb1")}})
	assert.Nil(t, err)
	r, err := s.Get(nil, "aa")
	assert.Nil(t, err)
	assert.Equal(t, "aa1", string(r.Value))
	r, err = s.Get(nil, "bb")
	assert.Nil(t, err)
	assert.Equal(t, "bb1", string(r.Value))
}

func TestPkvs_GetMany(t *testing.T) {
	s := New()
	recs := []kvs.Record{
		{
			Key:       "aaa",
			Value:     []byte("bbbb"),
			Version:   "ha ha",
			ExpiresAt: cast.Ptr(time.Now().Add(time.Millisecond * 100)),
		},
		{
			Key:     "aaa1",
			Value:   []byte("bbbb1"),
			Version: "ha ha",
		},
	}
	assert.Nil(t, s.PutMany(context.Background(), recs))

	recs1, err := s.GetMany(context.Background(), "aaa", "aaa1")
	assert.Nil(t, err)
	assert.Len(t, recs1, 2)
	assert.Equal(t, "aaa", recs1[0].Key)
	assert.Equal(t, "bbbb", string(recs1[0].Value))

	time.Sleep(time.Millisecond * 100)
	recs1, err = s.GetMany(context.Background(), "aaa", "aaa1")
	assert.Nil(t, err)
	assert.Len(t, recs1, 2)
	assert.Nil(t, recs1[0])
	assert.Equal(t, "bbbb1", string(recs1[1].Value))
}

func TestService_CasByVersion(t *testing.T) {
	s := New()
	r := kvs.Record{Key: "aa"}
	v, err := s.Create(context.Background(), r)
	assert.Nil(t, err)

	r.Value = []byte("ddd")
	r.Version = v
	r2, err := s.CasByVersion(nil, r)
	assert.Nil(t, err)
	assert.Equal(t, r2.Value, r.Value)

	_, err = s.CasByVersion(nil, r)
	assert.Equal(t, errors.ErrConflict, err)

	r.Key = "ddd"
	r.ExpiresAt = cast.Ptr(time.Now().Add(-time.Millisecond))
	v, err = s.Create(context.Background(), r)
	assert.Nil(t, err)
	r.Version = v
	_, err = s.CasByVersion(nil, r)
	assert.Equal(t, errors.ErrNotExist, err)
}

func TestService_Delete(t *testing.T) {
	s := New()
	r := kvs.Record{Key: "aa"}
	_, err := s.Create(context.Background(), r)
	assert.Nil(t, err)

	assert.Nil(t, s.Delete(nil, r.Key))
	assert.Equal(t, errors.ErrNotExist, s.Delete(nil, r.Key))
}

func TestWaitForVersionChange(t *testing.T) {
	s := New().(*service)
	ctx, cancel := context.WithCancel(context.Background())
	assert.Equal(t, errors.ErrNotExist, s.WaitForVersionChange(ctx, "a", "lala"))
	r := kvs.Record{Key: "a"}
	ver, err := s.Create(context.Background(), r)
	assert.Nil(t, err)

	cancel()
	assert.Equal(t, ctx.Err(), s.WaitForVersionChange(ctx, "a", ver))
	assert.Equal(t, 0, len(s.verChange))

	ctx = context.Background()
	assert.Nil(t, s.WaitForVersionChange(ctx, "a", ver+"dd"))
	start := time.Now()
	go func() {
		time.Sleep(time.Millisecond * 50)
		r.Value = []byte("dd")
		s.Put(ctx, r)
	}()
	assert.Nil(t, s.WaitForVersionChange(ctx, "a", ver))
	assert.True(t, time.Now().After(start.Add(time.Millisecond*49)))

	assert.Equal(t, 0, len(s.verChange))
}

func TestService_ListKeys(t *testing.T) {
	keys := []string{"key1", "key2", "aaa", "ee", "ey"}
	sort.Strings(keys)
	s := New().(*service)

	for _, k := range keys {
		_, v := s.Create(context.Background(), kvs.Record{Key: k, Value: []byte(k)})
		assert.Nil(t, v)
	}

	it, err := s.ListKeys(context.Background(), "*")
	assert.Nil(t, err)
	res := []string{}
	for it.HasNext() {
		v, ok := it.Next()
		assert.True(t, ok)
		res = append(res, v)
	}
	sort.Strings(res)
	assert.Equal(t, keys, res)

	it, err = s.ListKeys(context.Background(), "k*")
	assert.Nil(t, err)
	res = []string{}
	for it.HasNext() {
		v, ok := it.Next()
		assert.True(t, ok)
		res = append(res, v)
	}
	sort.Strings(res)
	assert.Equal(t, []string{"key1", "key2"}, res)

	it, err = s.ListKeys(context.Background(), "*ey*")
	assert.Nil(t, err)
	res = []string{}
	for it.HasNext() {
		v, ok := it.Next()
		assert.True(t, ok)
		res = append(res, v)
	}
	sort.Strings(res)
	assert.Equal(t, []string{"ey", "key1", "key2"}, res)

	it, err = s.ListKeys(context.Background(), "ddd")
	assert.Nil(t, err)
	res = []string{}
	for it.HasNext() {
		v, ok := it.Next()
		assert.True(t, ok)
		res = append(res, v)
	}
	sort.Strings(res)
	assert.Equal(t, []string{}, res)
}
