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
	"github.com/solarisdb/solaris/golibs/container/lru"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"os"
	"path/filepath"
)

// Provider manages a pull of opened chunks and allows to return a Chunk object by request.
// The Provider limits the number of opened file descriptors and the space on the local drive
// borrowed for the chunks
type Provider struct {
	logger logging.Logger
	dir    string
	chunks *lru.ReleasableCache[string, *Chunk]
}

// NewProvider creates the new Provider instance
func NewProvider(dir string, maxOpenedChunks int) *Provider {
	p := new(Provider)
	p.logger = logging.NewLogger("chunkfs.Provider")
	p.dir = dir
	var err error
	p.chunks, err = lru.NewReleasableCache[string, *Chunk](maxOpenedChunks, p.openChunk, p.closeChunk)
	if err != nil {
		panic(err)
	}
	return p
}

// GetOpenedChunk returns a lru.Releasable object for the *Chunk (ready to be used) by its ID.
// The function may return ctx.Err() or ErrClosed errors
func (p *Provider) GetOpenedChunk(ctx context.Context, id string) (lru.Releasable[*Chunk], error) {
	return p.chunks.GetOrCreate(ctx, id)
}

// Close implements the io.Closer
func (p *Provider) Close() error {
	p.logger.Infof("Close() called")
	return p.chunks.Close()
}

// ReleaseChunk must be called as soon as the chunk is not needed anymore
func (p *Provider) ReleaseChunk(r *lru.Releasable[*Chunk]) {
	p.chunks.Release(r)
}

func (p *Provider) openChunk(ctx context.Context, cID string) (*Chunk, error) {
	c := NewChunk(p.getFileNameByID(cID), cID)
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

	return c, err
}

func (p *Provider) closeChunk(cID string, c *Chunk) {
	if err := c.Close(); err != nil {
		p.logger.Warnf("could not close chunk c=%v", c)
	}
}

func (p *Provider) getFileNameByID(id string) string {
	return filepath.Join(p.getPathByID(id), id)
}

func (p *Provider) getPathByID(id string) string {
	ln := len(id)
	return filepath.Join(p.dir, id[ln-2:ln])
}
