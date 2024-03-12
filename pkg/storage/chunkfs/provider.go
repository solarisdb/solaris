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
	"context"
	"fmt"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// Provider manages a pull of opened chunks and allows to return a Chunk object by request.
// The Provider limits the number of opened file descriptors and the space on the local drive
// borrowed for the chunks
type Provider struct {
	lock   sync.Mutex
	cond   *sync.Cond
	logger logging.Logger
	dir    string
	chunks map[string]*Chunk
	active int
	maxFDs int
	// standBy is the collection of opened Chunks that maybe removed
	standBy *iterable.Map[string, *Chunk]
	// toOpenList the list of chunks that have to be opened
	toOpenList []*Chunk
	closed     bool
	closedCh   chan struct{}
}

// NewProvider creates the new Provider instance
func NewProvider(dir string, maxOpenedChunks int) *Provider {
	if maxOpenedChunks <= 0 {
		panic(fmt.Sprintf("expecting maxOpenedChunks=%d be positive", maxOpenedChunks))
	}
	p := new(Provider)
	p.cond = sync.NewCond(&p.lock)
	p.chunks = make(map[string]*Chunk)
	p.logger = logging.NewLogger("chunkfs.Provider")
	p.dir = dir
	p.maxFDs = maxOpenedChunks
	p.standBy = iterable.NewMap[string, *Chunk]()
	p.closedCh = make(chan struct{})
	go p.opener()
	return p
}

// GetOpenedChunk returns an opened chunk (ready to be used) by its ID.
// The function may return ctx.Err() or ErrClosed errors
func (p *Provider) GetOpenedChunk(ctx context.Context, id string) (*Chunk, error) {
	c, err := p.getOrCreateChunk(id)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		p.ReleaseChunk(c)
		return nil, ctx.Err()
	case <-p.closedCh:
		p.ReleaseChunk(c)
		return nil, errors.ErrClosed
	case <-c.neverOpened:
	}
	return c, nil
}

// Close implements the io.Closer
func (p *Provider) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}

	p.logger.Infof("closing provider: standBy=%d, chunks=%d, toOpenList=%d", p.standBy.Len(), len(p.chunks), len(p.toOpenList))

	it := p.standBy.Iterator()
	defer it.Close()
	for it.HasNext() {
		e, _ := it.Next()
		e.Value.Close()
	}
	p.standBy = nil
	p.chunks = nil
	p.toOpenList = nil

	p.closed = true
	close(p.closedCh)
	p.cond.Broadcast()
	return nil
}

func (p *Provider) getOrCreateChunk(id string) (*Chunk, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil, errors.ErrClosed
	}
	c, ok := p.chunks[id]
	if ok {
		if c.requested == 0 {
			// c should be in the standBy list, remove it out of there
			p.standBy.Remove(id)
			if !c.isOpened() {
				p.logger.Warnf("the chunk=%v found in standBy list, but it is not opened, will try to open")
				p.toOpenList = append(p.toOpenList, c)
				p.cond.Signal()
			}
		}
	} else {
		c = NewChunk(p.getFileNameByID(id), id)
		p.chunks[id] = c
		p.toOpenList = append(p.toOpenList, c)
		p.cond.Signal()
		p.logger.Debugf("New chunk is created %v", c)
	}
	atomic.AddInt32(&c.requested, 1)
	return c, nil
}

// ReleaseChunk must be called as soon as the chunk is not needed anymore
func (p *Provider) ReleaseChunk(c *Chunk) {
	p.lock.Lock()
	defer p.lock.Unlock()
	res := atomic.AddInt32(&c.requested, -1)
	if res < 0 {
		panic(fmt.Sprintf("ReleaseChunk() called more than requested for id=%s", c.id))
	}
	if res == 0 {
		if p.closed {
			p.logger.Debugf("ReleaseChunk chunk=%v, but the provider is closed", c)
			// just close the chunk if the provider is already closed
			c.Close()
			return
		}
		p.standBy.Add(c.id, c)
		if len(p.toOpenList) > 0 {
			// signal the opener just in case
			p.cond.Signal()
		}
	}
}

// opener is responsible for opening new chunks
func (p *Provider) opener() {
	p.logger.Infof("opener started")
	defer p.logger.Infof("opener ended")
	for {
		c := p.getChunkToOpen()
		if c == nil {
			// closed
			return
		}
		p.logger.Debugf("opening chunk %v", c)
		err := c.Open(false)
		if errors.Is(err, errCorrupted) {
			p.logger.Warnf("tried to open the chunk=%v, but got an corrupted error, will remove the file and try again: %v", c, err)
			_ = os.Remove(c.fn)
			err = c.Open(false)
		} else if errors.Is(err, errors.ErrNotExist) {
			pth := p.getPathByID(c.id)
			p.logger.Warnf("probably the folder %s doesn't exist, try to create a new one and open the chunk again: %v", pth, err)
			if err = files.EnsureDirExists(pth); err != nil {
				p.logger.Errorf("could not create the foler=%s, giving up", pth, err)
			} else {
				err = c.Open(false)
			}
		}

		if err != nil {
			p.logger.Errorf("could not open the chunk=%v. Unrecoverable error, will give up with the chunk for awhile: %v", c, err)
		} else {
			p.logger.Infof("the chunk=%v is opened ok", c)
		}
	}
}

func (p *Provider) getChunkToOpen() *Chunk {
	p.lock.Lock()
	defer p.lock.Unlock()

	for !p.closed {
		if len(p.toOpenList) == 0 {
			// waiting till a new chunk be requested to be opened
			p.cond.Wait()
			continue
		}

		if p.active == p.maxFDs {
			// the number of open chunks hits the limit
			if p.standBy.Len() > 0 {
				// some are opened and not used, let's close the least used one
				id, _ := p.standBy.First()
				c := p.chunks[id]
				p.standBy.Remove(id)
				delete(p.chunks, id)
				p.logger.Debugf("closing channel %v due to the file descriptors starvation ", c)
				go func() {
					c.Close()
				}()
				p.active--
			} else {
				// let's wait until something happens
				p.cond.Wait()
				continue
			}
		}
		// borrow one counter
		p.active++
		last := len(p.toOpenList) - 1
		c := p.toOpenList[last]
		p.toOpenList[last] = nil
		p.toOpenList = p.toOpenList[:last]

		if _, ok := p.standBy.Get(c.id); ok {
			p.logger.Debugf("found the chunk=%v in the toOpenList, but all requestors are gone, will not open it now.", c)
			// the chunk was requested to be opened, but all clients, who wanted to work with it, gone.
			// Remove it from the lists
			p.standBy.Remove(c.id)
			delete(p.chunks, c.id)
			go func() {
				c.Close()
			}()
			// return the counter back
			p.active--
			continue
		}
		return c
	}
	return nil
}

func (p *Provider) getFileNameByID(id string) string {
	return filepath.Join(p.getPathByID(id), id)
}

func (p *Provider) getPathByID(id string) string {
	ln := len(id)
	return filepath.Join(p.dir, id[ln-2:ln])
}
