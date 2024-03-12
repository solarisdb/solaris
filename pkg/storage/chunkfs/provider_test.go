package chunkfs

import (
	context2 "context"
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"
)

func TestProvider_closed(t *testing.T) {
	p := NewProvider("", 1)
	p.Close()
	c, err := p.GetOpenedChunk(context2.Background(), "la la")
	assert.Nil(t, c)
	assert.True(t, errors.Is(err, errors.ErrClosed))
}

func TestProvider_newPanics(t *testing.T) {
	assert.Panics(t, func() { NewProvider("", 0) })
}

func TestProvider_lifeCycle(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestProvider_lifeCycle")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := NewProvider(dir, 1)
	c, err := p.GetOpenedChunk(context2.Background(), "lala")
	assert.Nil(t, err)
	assert.Equal(t, 0, p.standBy.Len())
	assert.Equal(t, 0, len(p.toOpenList))
	assert.Equal(t, 1, len(p.chunks))
	assert.Equal(t, 1, p.active)
	assert.NotNil(t, p.chunks[c.id])

	p.ReleaseChunk(c)
	assert.Equal(t, 1, p.active)
	assert.Equal(t, 1, p.standBy.Len())
	assert.Equal(t, 1, len(p.chunks))

	c, err = p.GetOpenedChunk(context2.Background(), "bbbb")
	assert.Nil(t, err)
	assert.Equal(t, 1, p.active)
	assert.Equal(t, 0, p.standBy.Len())
	assert.Equal(t, 0, len(p.toOpenList))
	assert.Equal(t, 1, len(p.chunks))
	assert.NotNil(t, p.chunks[c.id])

	var wg sync.WaitGroup
	wg.Add(1)
	var c2 *Chunk
	go func() {
		c2, err = p.GetOpenedChunk(context2.Background(), "lala")
		wg.Done()
	}()
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 1, len(p.toOpenList))

	p.ReleaseChunk(c)
	wg.Wait()
	assert.Equal(t, 1, p.active)
	assert.Equal(t, 0, p.standBy.Len())
	assert.Equal(t, 1, len(p.chunks))
	assert.Equal(t, 0, len(p.toOpenList))
	assert.NotNil(t, p.chunks[c2.id])

	p.ReleaseChunk(c2)
	assert.Equal(t, 1, p.active)
	assert.Equal(t, 1, p.standBy.Len())
	assert.Equal(t, 1, len(p.chunks))

	c2, err = p.GetOpenedChunk(context2.Background(), "lala")
	assert.Nil(t, err)
	assert.NotNil(t, c2)
	go func() {
		assert.Equal(t, 1, p.active)
		p.Close()
	}()
	c, err = p.GetOpenedChunk(context2.Background(), "bbbb")
	assert.Equal(t, errors.ErrClosed, err)
	assert.Nil(t, c)

	assert.NotNil(t, c2.mmf)
	p.ReleaseChunk(c2)
	assert.Nil(t, c2.mmf)

	assert.Nil(t, p.standBy)
	assert.Nil(t, p.chunks)
	assert.Nil(t, p.toOpenList)
}

func TestProvider_contextClosed(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestProvider_contextClosed")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := NewProvider(dir, 1)
	defer p.Close()

	c, err := p.GetOpenedChunk(context2.Background(), "lala")
	assert.Nil(t, err)
	assert.Equal(t, 0, p.standBy.Len())
	assert.Equal(t, 0, len(p.toOpenList))
	assert.Equal(t, 1, len(p.chunks))
	assert.Equal(t, 1, p.active)

	ctx, cancel := context2.WithTimeout(context2.Background(), 100*time.Millisecond)
	defer cancel()
	for i := 0; i < 10; i++ {
		go func() {
			p.GetOpenedChunk(ctx, fmt.Sprintf("b%dbb", i))
		}()
	}
	c2, err := p.GetOpenedChunk(ctx, "bbbb")
	assert.Nil(t, c2)
	assert.Equal(t, ctx.Err(), err)
	time.Sleep(time.Millisecond * 100)

	p.ReleaseChunk(c)
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 1, p.standBy.Len())
	assert.Equal(t, 0, len(p.toOpenList))
	assert.Equal(t, 1, len(p.chunks))
	assert.NotNil(t, p.chunks[c.id])
	assert.Equal(t, 1, p.active)

}
