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
package redis

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/kvs"
	"github.com/stretchr/testify/assert"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type (
	testClient struct {
		*client
		mini *miniredis.Miniredis
	}
)

const expTimeout = time.Second

func TestService_Create(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aa")

	r := kvs.Record{Key: "aa"}
	v, err := c.Create(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, v, r.Version)

	_, err = c.Create(context.Background(), r)
	assert.Equal(t, errors.ErrExist, err)
}

func TestPkvs_Get(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")

	_, err := c.Get(context.Background(), "aaa")
	assert.Equal(t, errors.ErrNotExist, err)

	r := kvs.Record{Key: "aaa", Value: []byte("bbbb"), Version: "ha ha"}
	v, err := c.Create(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, "ha ha", v)
	r.Version = v

	r1, err := c.Get(context.Background(), "aaa")
	assert.Nil(t, err)
	assert.Equal(t, r, r1)
}

func TestPkvs_GetExpired(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	r := kvs.Record{
		Key:       "aaa",
		Value:     []byte("bbbb"),
		Version:   "ha ha",
		ExpiresAt: cast.Ptr(time.Now().Add(expTimeout)),
	}
	v, err := c.Create(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, "ha ha", v)
	r.Version = v

	c.goForward(expTimeout)

	_, err = c.Get(context.Background(), "aaa")
	assert.Equal(t, errors.ErrNotExist, err)
}

func TestPkvs_Put(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	r := kvs.Record{
		Key:     "aaa",
		Value:   []byte("bbbb"),
		Version: "ha ha",
	}
	r1, err := c.Put(context.Background(), r)
	assert.Nil(t, err)
	r2, err := c.Get(context.Background(), r.Key)
	assert.Nil(t, err)
	assert.Equal(t, r1, r2)

	r.Value = []byte("ddd")
	r1, err = c.Put(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, r1.Version, r.Version)
	r.Version = r1.Version
	assert.Equal(t, r1, r)

	r, err = c.Get(context.Background(), r1.Key)
	assert.Nil(t, err)
	assert.Equal(t, r1, r)
}

func TestPkvs_PutMany(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	recs := []kvs.Record{
		{
			Key:     "aaa",
			Value:   []byte("bbbb"),
			Version: "ha ha",
		},
		{
			Key:     "aaa1",
			Value:   []byte("bbbb1"),
			Version: "ha ha",
		},
	}
	assert.Nil(t, c.PutMany(context.Background(), recs))

	recs[0].Value = []byte("ddd")
	assert.Nil(t, c.PutMany(context.Background(), recs))

	r1, err := c.Get(context.Background(), recs[0].Key)
	assert.Nil(t, err)
	assert.Equal(t, recs[0].Value, r1.Value)
}

func TestPkvs_PutManyExpired(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	recs := []kvs.Record{
		{
			Key:       "aaa",
			Value:     []byte("bbbb"),
			Version:   "ha ha",
			ExpiresAt: cast.Ptr(time.Now().Add(expTimeout)),
		},
		{
			Key:     "aaa1",
			Value:   []byte("bbbb1"),
			Version: "ha ha",
		},
	}
	start := time.Now()
	assert.Nil(t, c.PutMany(context.Background(), recs))

	r1, err := c.Get(context.Background(), recs[0].Key)
	assert.Nil(t, err)
	assert.Equal(t, recs[0].Value, r1.Value)
	assert.True(t, (*r1.ExpiresAt).After(start))

	c.goForward(expTimeout)

	_, err = c.Get(context.Background(), recs[0].Key)
	assert.True(t, errors.Is(err, errors.ErrNotExist))
}

func TestPkvs_GetManyExpired(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	recs := []kvs.Record{
		{
			Key:       "aaa",
			Value:     []byte("bbbb"),
			Version:   "ha ha",
			ExpiresAt: cast.Ptr(time.Now().Add(expTimeout)),
		},
		{
			Key:     "aaa1",
			Value:   []byte("bbbb1"),
			Version: "ha ha",
		},
	}
	assert.Nil(t, c.PutMany(context.Background(), recs))

	recs1, err := c.GetMany(context.Background(), "aaa", "aaa1")
	assert.Nil(t, err)
	assert.Len(t, recs1, 2)
	assert.Equal(t, "aaa", recs1[0].Key)
	assert.Equal(t, "bbbb", string(recs1[0].Value))

	c.goForward(expTimeout)

	recs1, err = c.GetMany(context.Background(), "aaa", "aaa1")
	assert.Nil(t, err)
	assert.Len(t, recs1, 2)
	assert.Nil(t, recs1[0])
	assert.Equal(t, "bbbb1", string(recs1[1].Value))
}

func TestPkvs_CasByVersion(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	r := kvs.Record{Key: "aaa", Value: []byte("bbbb"), Version: "ha ha", ExpiresAt: cast.Ptr(time.Now().Add(10 * time.Second))}
	v, err := c.Create(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, "ha ha", v)

	r, err = c.Get(context.Background(), "aaa")
	assert.Nil(t, err)

	r.Value = []byte("ddd")
	r, err = c.CasByVersion(context.Background(), r)
	assert.Nil(t, err)

	r.Version = "bad day"
	_, err = c.CasByVersion(context.Background(), r)
	assert.Equal(t, errors.ErrConflict, err)
}

func TestPkvs_CasByVersion_Stress(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	rec := kvs.Record{Key: "aaa", Value: []byte("bbbb"), Version: "ha ha", ExpiresAt: cast.Ptr(time.Now().Add(10 * time.Second))}
	v, err := c.Create(context.Background(), rec)
	assert.Nil(t, err)
	assert.NotEqual(t, "ha ha", v)

	rec, err = c.Get(context.Background(), "aaa")
	assert.Nil(t, err)

	total := 1000
	before := sync.WaitGroup{}
	before.Add(total)
	after := sync.WaitGroup{}
	after.Add(total)
	var count int32
	for i := 0; i < total; i++ {
		go func(i int, r kvs.Record) {
			before.Done()
			before.Wait()
			_, err = c.CasByVersion(context.Background(), r)
			if err == nil {
				atomic.AddInt32(&count, 1)
			}
			after.Done()
		}(i, rec)
	}

	after.Wait()
	assert.Equal(t, int32(1), count)
}

func TestPkvs_CasByVersionExpired(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	r := kvs.Record{
		Key:       "aaa",
		Value:     []byte("bbbb"),
		Version:   "ha ha",
		ExpiresAt: cast.Ptr(time.Now().Add(expTimeout)),
	}
	v, err := c.Create(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, "ha ha", v)

	c.goForward(expTimeout)

	r.Value = []byte("ddd")
	r.Version = v
	r, err = c.CasByVersion(context.Background(), r)
	assert.Equal(t, errors.ErrNotExist, err)
	_, err = c.Get(context.Background(), "aaa")
	assert.Equal(t, errors.ErrNotExist, err)
}

func TestPkvs_Delete(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")
	r := kvs.Record{Key: "aaa", Value: []byte("bbbb"), Version: "ha ha"}
	v, err := c.Create(context.Background(), r)
	assert.Nil(t, err)
	assert.NotEqual(t, "ha ha", v)

	assert.Nil(t, c.Delete(context.Background(), "aaa"))
	_, err = c.Get(context.Background(), "aaa")
	assert.Equal(t, errors.ErrNotExist, err)
	assert.Equal(t, errors.ErrNotExist, c.Delete(context.Background(), "aaa"))
}

func TestClient_CasByVersion2(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "aaa")

	assert.Equal(t, errors.ErrNotExist, c.Delete(context.Background(), "aaa"))
	r := kvs.Record{Key: "aaa", Version: "ddd", Value: []byte{33}}
	r, err := c.Put(context.Background(), r)
	assert.Nil(t, err)
	r.Value = []byte{55}
	_, err = c.CasByVersion(context.Background(), r)
	assert.Nil(t, err)
	_, err = c.CasByVersion(context.Background(), r)
	assert.True(t, errors.Is(err, errors.ErrConflict))
}

func Test_rec2db(t *testing.T) {
	var r kvs.Record
	r.Key = "asdf"
	r.Value = []byte{1, 3, 4}
	r.ExpiresAt = cast.Ptr(time.Now())
	r1 := db2rec(rec2db(&r))
	assert.Equal(t, r1.ExpiresAt.Nanosecond(), r.ExpiresAt.Nanosecond())
	r1.ExpiresAt = r.ExpiresAt
	assert.Equal(t, r, r1)
}

func TestClient_WaitForVersionChange(t *testing.T) {
	c := newClient(t)
	defer c.Delete(context.Background(), "a")

	ctx, cancel := context.WithCancel(context.Background())
	assert.Equal(t, errors.ErrNotExist, c.WaitForVersionChange(ctx, "a", "lala"))
	r := kvs.Record{Key: "a"}
	ver, err := c.Create(ctx, r)
	assert.Nil(t, err)

	cancel()
	assert.Equal(t, ctx.Err(), c.WaitForVersionChange(ctx, "a", ver))

	ctx = context.Background()
	assert.Nil(t, c.WaitForVersionChange(ctx, "a", ver+"dd"))
	start := time.Now()
	go func() {
		time.Sleep(time.Millisecond * 50)
		r.Value = []byte("dd")
		c.Put(ctx, r)
	}()
	assert.Nil(t, c.WaitForVersionChange(ctx, "a", ver))
	assert.True(t, time.Now().After(start.Add(time.Millisecond*49)))
}

func TestClient_ListKeys(t *testing.T) {
	keys := []string{"key1", "key2", "aaa", "ee", "ey"}
	sort.Strings(keys)
	c := newClient(t)
	defer func() {
		for _, k := range keys {
			c.Delete(context.Background(), k)
		}
	}()
	c.rdb.FlushAll(context.Background())

	for _, k := range keys {
		_, v := c.Create(context.Background(), kvs.Record{Key: k, Value: []byte(k)})
		assert.Nil(t, v)
	}

	it, err := c.ListKeys(context.Background(), "*")
	assert.Nil(t, err)
	res := []string{}
	for it.HasNext() {
		v, ok := it.Next()
		assert.True(t, ok)
		res = append(res, v)
	}
	sort.Strings(res)
	assert.Equal(t, keys, res)

	it, err = c.ListKeys(context.Background(), "k*")
	assert.Nil(t, err)
	res = []string{}
	for it.HasNext() {
		v, ok := it.Next()
		assert.True(t, ok)
		res = append(res, v)
	}
	sort.Strings(res)
	assert.Equal(t, []string{"key1", "key2"}, res)

	it, err = c.ListKeys(context.Background(), "*ey*")
	assert.Nil(t, err)
	res = []string{}
	for it.HasNext() {
		v, ok := it.Next()
		assert.True(t, ok)
		res = append(res, v)
	}
	sort.Strings(res)
	assert.Equal(t, []string{"ey", "key1", "key2"}, res)

	it, err = c.ListKeys(context.Background(), "ddd")
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

func Test_record2grpcRecord(t *testing.T) {
	r := kvs.Record{
		Key:     "aaa",
		Value:   []byte("bbb"),
		Version: "fff",
	}
	assert.Equal(t, r, ProtoRecord2Record(Record2protoRecord(&r)))
	prev := Record2protoRecord(&r)
	r.ExpiresAt = cast.Ptr(time.Now().UTC())
	assert.NotEqual(t, prev, Record2protoRecord(&r))
	assert.Equal(t, r, ProtoRecord2Record(Record2protoRecord(&r)))
}

func Test_expiration(t *testing.T) {
	curT := time.Now()
	assert.Equal(t, time.Duration(0), expiration(nil, curT))
	assert.Equal(t, time.Millisecond, expiration(cast.Ptr(curT.Truncate(time.Second)), curT))
	assert.Equal(t, time.Second, expiration(cast.Ptr(curT.Add(time.Second)), curT))
}

func newClient(t *testing.T) *testClient {
	return newMini(t)
	return newRealRedis()
}

func newMini(t *testing.T) *testClient {
	s := miniredis.RunT(t)
	return &testClient{client: New(&redis.Options{Addr: s.Addr()}).(*client),
		mini: s}
}

func (c *testClient) goForward(to time.Duration) {
	if c.mini != nil {
		c.mini.FastForward(to)
		return
	}
	time.Sleep(to)
}

func newRealRedis() *testClient {
	return &testClient{client: New(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}).(*client)}
}
