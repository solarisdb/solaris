// Copyright 2024 The Solaris Authors
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
	"context"
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkRCache_GetOrCreate_NoMisses(b *testing.B) {
	r, _ := NewReleasableCache(1, func(_ context.Context, k string) (string, error) {
		return "bb", nil
	}, nil)

	r.GetOrCreate(context.Background(), "aa")
	b.ResetTimer()
	b.ReportAllocs()
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		rl, _ := r.GetOrCreate(ctx, "aa")
		r.Release(&rl)
	}
}

func BenchmarkRCache_GetOrCreate_Misses(b *testing.B) {
	r, _ := NewReleasableCache(1000, func(_ context.Context, k int) (string, error) {
		return "bb", nil
	}, nil)

	// We have 1000 elements in cache, but only 1/3 of requests should hit the cache
	rnd := rand.New(rand.NewSource(time.Now().UnixMicro()))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rl, _ := r.GetOrCreate(context.Background(), rnd.Intn(3000))
		r.Release(&rl)
	}
}

func BenchmarkRCache_GetOrCreate_Full(b *testing.B) {
	r, _ := NewReleasableCache(1000, func(_ context.Context, k int) (string, error) {
		return "bb", nil
	}, nil)

	// We have 1000 elements in cache, but only 1/3 of requests should hit the cache
	rnd := rand.New(rand.NewSource(time.Now().UnixMicro()))

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rl, _ := r.GetOrCreate(context.Background(), rnd.Intn(1000))
		r.Release(&rl)
	}
}

func TestNewReleasableCache(t *testing.T) {
	r, err := NewReleasableCache[string, string](1, func(_ context.Context, k string) (string, error) {
		return "bb", nil
	}, nil)
	assert.Nil(t, err)
	rl, err := r.GetOrCreate(context.Background(), "aa")
	assert.Equal(t, "bb", rl.Value())
	assert.Nil(t, err)

	_, err = NewReleasableCache[string, string](0, func(_ context.Context, k string) (string, error) { return "", nil }, nil)
	assert.NotNil(t, err)
	_, err = NewReleasableCache[string, string](1, nil, nil)
	assert.NotNil(t, err)
}

func TestReleasableCache_GetOrCreateSimple(t *testing.T) {
	cnt := 0
	p, err := NewReleasableCache[string, int](1, func(_ context.Context, k string) (int, error) {
		cnt++
		return cnt, nil
	}, nil)
	assert.Nil(t, err)
	rl, err := p.GetOrCreate(context.Background(), "aa")
	assert.Equal(t, 1, rl.Value())
	assert.Nil(t, err)
	p.Release(&rl)

	rl, err = p.GetOrCreate(context.Background(), "aa")
	assert.Equal(t, 1, rl.Value())
	assert.Nil(t, err)
	assert.Equal(t, 0, p.lruCache.Len())
	p.Release(&rl)
	assert.Equal(t, 1, p.lruCache.Len())

	assert.Equal(t, 1, cnt)

	rl, err = p.GetOrCreate(context.Background(), "bb")
	assert.Equal(t, 2, rl.Value())
	assert.Nil(t, err)
	assert.Equal(t, 1, len(p.allKnown))
	assert.Equal(t, 0, len(p.inflight))
	assert.Equal(t, 0, p.lruCache.Len())
	assert.Equal(t, 2, cnt)
	p.Release(&rl)
	assert.Equal(t, 1, p.lruCache.Len())
}

func TestReleasableCache_GetOrCreate(t *testing.T) {
	ch := make(chan struct{})
	cnt := int32(0)
	f := func(_ context.Context, k int) (int, error) {
		res := atomic.AddInt32(&cnt, 1)
		<-ch
		return int(res), nil
	}
	p, err := NewReleasableCache[int, int](2, f, nil)
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
			rl, err := p.GetOrCreate(context.Background(), 23)
			assert.Nil(t, err)
			p.Release(&rl)
		}()
	}

	res, err := p.GetOrCreate(context.Background(), 23)
	assert.Nil(t, err)
	assert.Equal(t, 1, res.Value())
	wg.Wait()
	assert.Equal(t, 0, len(p.inflight))
	assert.Equal(t, 0, p.lruCache.Len())
	p.Release(&res)
	assert.Equal(t, 1, p.lruCache.Len())
}

func TestReleasableCache_Waiter(t *testing.T) {
	cnt := 0
	p, err := NewReleasableCache[string, int](1, func(_ context.Context, k string) (int, error) {
		cnt++
		return cnt, nil
	}, nil)
	assert.Nil(t, err)
	rl, err := p.GetOrCreate(context.Background(), "aa")
	assert.Equal(t, 1, rl.Value())
	assert.Nil(t, err)
	go func() {
		time.Sleep(time.Millisecond * 50)
		p.Release(&rl)
	}()
	rl1, err := p.GetOrCreate(context.Background(), "bb")
	assert.Equal(t, 2, rl1.Value())
	assert.Nil(t, err)
	assert.Equal(t, 0, len(p.inflight))
	assert.Equal(t, 0, p.lruCache.Len())
	assert.Equal(t, 1, len(p.allKnown))
	p.Release(&rl1)
}

func TestReleasableCache_ManyWaiter(t *testing.T) {
	p, err := NewReleasableCache[string, string](1, func(_ context.Context, k string) (string, error) {
		return k, nil
	}, nil)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			time.Sleep(10 * time.Millisecond)
			s := fmt.Sprintf("%d", i)
			rl, err := p.GetOrCreate(context.Background(), s)
			assert.Nil(t, err)
			assert.Equal(t, s, rl.Value())
			p.Release(&rl)
			wg.Done()
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 0, len(p.inflight))
	assert.Equal(t, 1, p.lruCache.Len())
	assert.Equal(t, 1, len(p.allKnown))
	assert.Nil(t, p.waiter)
}

func TestReleasableCache_WaitUntilCreated(t *testing.T) {
	ch := make(chan struct{})
	p, err := NewReleasableCache[string, string](1, func(_ context.Context, k string) (string, error) {
		<-ch
		return k, nil
	}, nil)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			rl, err := p.GetOrCreate(context.Background(), "aaa")
			assert.Nil(t, err)
			assert.Equal(t, "aaa", rl.Value())
			assert.True(t, time.Now().Sub(start) > time.Millisecond*50)

			p.Release(&rl)
			wg.Done()
		}(i)
	}
	time.Sleep(time.Millisecond * 50)
	close(ch)
	wg.Wait()
	assert.Equal(t, 0, len(p.inflight))
	assert.Equal(t, 1, p.lruCache.Len())
	assert.Equal(t, 1, len(p.allKnown))
	assert.Nil(t, p.waiter)
}

func TestReleasableCache_GetOrCreateError(t *testing.T) {
	cnt := 0
	f := func(_ context.Context, k int) (int, error) {
		cnt++
		return 0, os.ErrClosed
	}
	p, err := NewReleasableCache[int, int](2, f, nil)
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		_, err := p.GetOrCreate(context.Background(), 1)
		assert.ErrorIs(t, err, os.ErrClosed)
	}
	assert.Equal(t, 10, cnt)
}

func TestReleasableCache_CloseContextWhileWaitingCreation(t *testing.T) {
	ch := make(chan struct{})
	f := func(_ context.Context, k int) (int, error) {
		<-ch
		return 0, os.ErrInvalid
	}
	p, err := NewReleasableCache[int, int](1, f, nil)
	assert.Nil(t, err)

	go func() {
		_, err := p.GetOrCreate(context.Background(), 1)
		assert.Equal(t, errors.ErrInvalid, err)
	}()
	// let the goroutine above to start
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	_, err = p.GetOrCreate(ctx, 1)
	assert.NotNil(t, err)
	assert.Equal(t, ctx.Err(), err)
	assert.Equal(t, 1, len(p.inflight))
	close(ch)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, p.lruCache.Len())
	assert.Equal(t, 0, len(p.allKnown))
	assert.Equal(t, 0, len(p.inflight))
}

func TestReleasableCache_CloseContextWhileNoSpace(t *testing.T) {
	f := func(_ context.Context, k int) (int, error) {
		return k, nil
	}
	p, err := NewReleasableCache[int, int](1, f, nil)
	assert.Nil(t, err)

	rl, err := p.GetOrCreate(context.Background(), 1)
	assert.Equal(t, 1, rl.Value())
	assert.Nil(t, err)

	// let the goroutine above to start
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	_, err = p.GetOrCreate(ctx, 2)
	assert.NotNil(t, err)
	assert.Equal(t, ctx.Err(), err)
	p.Release(&rl)
	rl, err = p.GetOrCreate(ctx, 2)
	assert.Equal(t, 2, rl.Value())
	assert.Nil(t, err)
}

func TestReleasableCache_Close(t *testing.T) {
	m := make(map[int]int)
	df := func(k, v int) {
		m[k] = v
	}
	f := func(_ context.Context, k int) (int, error) {
		return k, nil
	}
	p, err := NewReleasableCache[int, int](1, f, df)
	assert.Nil(t, err)

	rl, err := p.GetOrCreate(context.Background(), 1)
	assert.Equal(t, 1, rl.Value())
	assert.Nil(t, err)

	go func() {
		time.Sleep(time.Millisecond * 10)
		p.Close()
	}()
	_, err = p.GetOrCreate(context.Background(), 2)
	assert.True(t, errors.Is(err, errors.ErrClosed))
	assert.Equal(t, 0, len(m))
	p.Release(&rl)
	assert.Equal(t, 1, len(m))
	assert.Equal(t, 1, m[1])
}

func TestReleasableCache_Close1(t *testing.T) {
	ch := make(chan struct{})
	f := func(_ context.Context, k int) (int, error) {
		<-ch
		return k, nil
	}
	p, err := NewReleasableCache[int, int](1, f, nil)
	assert.Nil(t, err)

	go func() {
		time.Sleep(time.Millisecond * 50)
		p.Close()
	}()

	go func() {
		time.Sleep(time.Millisecond * 10)
		_, err := p.GetOrCreate(context.Background(), 1)
		assert.True(t, errors.Is(err, errors.ErrClosed))
		close(ch)
	}()
	_, err = p.GetOrCreate(context.Background(), 1)
	assert.True(t, errors.Is(err, errors.ErrClosed))
}
