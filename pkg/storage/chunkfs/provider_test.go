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
package chunkfs

import (
	context2 "context"
	"fmt"
	"github.com/solarisdb/solaris/golibs/container/lru"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func TestProvider_closed(t *testing.T) {
	p := NewProvider("", 1, GetDefaultConfig())
	p.Close()
	_, err := p.GetOpenedChunk(context2.Background(), "la la", true)
	assert.True(t, errors.Is(err, errors.ErrClosed))
}

func TestProvider_newPanics(t *testing.T) {
	assert.Panics(t, func() { NewProvider("", 0, GetDefaultConfig()) })
}

func TestProvider_lifeCycle(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestProvider_lifeCycle")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := NewProvider(dir, 1, GetDefaultConfig())
	_, err = p.GetOpenedChunk(context2.Background(), "lala", false)
	assert.NotNil(t, err)
	rc, err := p.GetOpenedChunk(context2.Background(), "lala", true)
	assert.Nil(t, err)
	p.ReleaseChunk(&rc)

	rc, err = p.GetOpenedChunk(context2.Background(), "bbbb", true)
	assert.Nil(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	var c2 lru.Releasable[*Chunk]
	go func() {
		c2, err = p.GetOpenedChunk(context2.Background(), "lala", true)
		wg.Done()
	}()
	time.Sleep(time.Millisecond * 100)

	p.ReleaseChunk(&rc)
	wg.Wait()

	p.ReleaseChunk(&c2)

	c2, err = p.GetOpenedChunk(context2.Background(), "lala", true)
	assert.Nil(t, err)
	assert.NotNil(t, c2)
	go func() {
		p.Close()
	}()
	rc, err = p.GetOpenedChunk(context2.Background(), "bbbb", true)
	assert.Equal(t, errors.ErrClosed, err)

	assert.NotNil(t, c2.Value().mmf)
	p.ReleaseChunk(&c2)
	assert.Nil(t, c2.Value().mmf)
}

func TestProvider_contextClosed(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestProvider_contextClosed")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := NewProvider(dir, 1, GetDefaultConfig())
	defer p.Close()

	c, err := p.GetOpenedChunk(context2.Background(), "lala", true)
	assert.Nil(t, err)

	ctx, cancel := context2.WithTimeout(context2.Background(), 100*time.Millisecond)
	defer cancel()
	for i := 0; i < 10; i++ {
		go func() {
			p.GetOpenedChunk(ctx, fmt.Sprintf("b%dbb", i), true)
		}()
	}
	_, err = p.GetOpenedChunk(ctx, "bbbb", true)
	assert.Equal(t, ctx.Err(), err)
	time.Sleep(time.Millisecond * 100)

	p.ReleaseChunk(&c)
	time.Sleep(time.Millisecond * 100)
}
