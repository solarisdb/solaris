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
package lru

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func BenchmarkCache_GetOrCreate_NoMisses(b *testing.B) {
	p, _ := NewCache(1, func(k string) (string, error) {
		return "bb", nil
	}, nil)

	p.GetOrCreate("aa")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.GetOrCreate("aa")
	}
}

func BenchmarkCache_GetOrCreate_Misses(b *testing.B) {
	p, _ := NewCache(1000, func(k int) (string, error) {
		return "bb", nil
	}, nil)

	// We have 1000 elements in cache, but only 1/3 of requests should hit the cache
	rnd := rand.New(rand.NewSource(time.Now().UnixMicro()))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.GetOrCreate(rnd.Intn(3000))
	}
}

func TestNewCache(t *testing.T) {
	p, err := NewCache[string, string](1, func(k string) (string, error) {
		return "bb", nil
	}, nil)
	assert.Nil(t, err)
	r, err := p.GetOrCreate("aa")
	assert.Equal(t, "bb", r)
	assert.Nil(t, err)

	_, err = NewCache[string, string](0, func(k string) (string, error) { return "", nil }, nil)
	assert.NotNil(t, err)
	_, err = NewCache[string, string](1, nil, nil)
	assert.NotNil(t, err)
}

func TestCache_GetOrCreateSimple(t *testing.T) {
	cnt := 0
	p, err := NewCache[string, int](1, func(k string) (int, error) {
		cnt++
		return cnt, nil
	}, nil)
	assert.Nil(t, err)
	r, err := p.GetOrCreate("aa")
	assert.Equal(t, 1, r)
	assert.Nil(t, err)

	r, err = p.GetOrCreate("aa")
	assert.Equal(t, 1, r)
	assert.Nil(t, err)

	assert.Equal(t, 1, cnt)

	r, err = p.GetOrCreate("bb")
	assert.Equal(t, 2, r)
	assert.Nil(t, err)
	assert.Equal(t, 1, p.items.Len())
	assert.Equal(t, 0, len(p.inflight))
	assert.Equal(t, 2, cnt)
}

func TestCache_GetOrCreate(t *testing.T) {
	ch := make(chan struct{})
	cnt := int32(0)
	f := func(k int) (int, error) {
		res := atomic.AddInt32(&cnt, 1)
		<-ch
		return int(res), nil
	}
	p, err := NewCache[int, int](2, f, nil)
	assert.Nil(t, err)

	c := int32(0)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if atomic.AddInt32(&c, 1) == 10 {
				close(ch)
				return
			}
			p.GetOrCreate(23)
		}()
	}

	res, err := p.GetOrCreate(23)
	assert.Equal(t, 1, res)
	assert.Nil(t, err)
	wg.Wait()
	assert.Equal(t, 0, len(p.inflight))
}

func TestCache_GetOrCreateError(t *testing.T) {
	cnt := 0
	f := func(k int) (int, error) {
		cnt++
		return 0, os.ErrClosed
	}
	p, err := NewCache[int, int](2, f, nil)
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		_, err := p.GetOrCreate(1)
		assert.ErrorIs(t, err, os.ErrClosed)
	}
	assert.Equal(t, 10, cnt)
}

func TestCache_CheckOrder(t *testing.T) {
	f := func(k int) (int, error) {
		return k, nil
	}
	p, err := NewCache[int, int](10, f, nil)
	assert.Nil(t, err)

	for i := 0; i < 20; i++ {
		p.GetOrCreate(i)
	}
	assert.Equal(t, 10, p.items.Len())
	it := p.items.Iterator()
	cnt := 10
	for it.HasNext() {
		e, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, e.Value.pk, e.Value.v)
		assert.Equal(t, e.Key, e.Value.v)
		assert.Equal(t, cnt, e.Value.v)
		cnt++
	}
}

func TestCache_CheckDelete(t *testing.T) {
	f := func(k int) (int, error) {
		return k, nil
	}
	deleted := []int{}
	d := func(k, v int) {
		deleted = append(deleted, v)
	}
	p, err := NewCache[int, int](10, f, d)
	assert.Nil(t, err)

	for i := 0; i < 20; i++ {
		p.GetOrCreate(i)
	}
	assert.Equal(t, 10, p.items.Len())
	it := p.items.Iterator()
	cnt := 10
	for it.HasNext() {
		e, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, e.Value.pk, e.Value.v)
		assert.Equal(t, e.Key, e.Value.v)
		assert.Equal(t, cnt, e.Value.v)
		cnt++
	}

	assert.Equal(t, 10, len(deleted))
	for i := 0; i < 10; i++ {
		assert.Equal(t, i, deleted[i])
	}
}

func TestCache_Remove(t *testing.T) {
	f := func(k int) (int, error) {
		return k, nil
	}
	deleted := []int{}
	d := func(k, v int) {
		deleted = append(deleted, v)
	}
	p, err := NewCache[int, int](20, f, d)
	assert.Nil(t, err)

	for i := 0; i < 20; i++ {
		p.GetOrCreate(i)
	}
	assert.Equal(t, 0, len(deleted))
	p.Remove(5)
	assert.Equal(t, []int{5}, deleted)
	p.Remove(35)
	assert.Equal(t, []int{5}, deleted)
}

func TestCache_Clear(t *testing.T) {
	f := func(k int) (int, error) {
		return k, nil
	}
	deleted := []int{}
	d := func(k, v int) {
		deleted = append(deleted, v)
	}
	p, err := NewCache[int, int](10, f, d)
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		p.GetOrCreate(i)
	}
	assert.Equal(t, 10, p.items.Len())
	assert.Equal(t, 10, p.Clear())
	assert.Equal(t, 0, p.items.Len())
	assert.Equal(t, 10, len(deleted))
	for i := 0; i < 10; i++ {
		assert.Equal(t, i, deleted[i])
	}

	p, err = NewCache[int, int](10, f, nil)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		p.GetOrCreate(i)
	}
	assert.Equal(t, 10, p.items.Len())
	assert.Equal(t, 10, p.Clear())
	assert.Equal(t, 0, p.items.Len())
}
