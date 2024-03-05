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
package context

import (
	ctx "context"
	errors2 "github.com/solarisdb/solaris/golibs/errors"
	"sync"
	"time"
)

type (
	cancelErrCtx struct {
		pCtx ctx.Context
		ch   chan struct{}
		err  error
		mu   sync.Mutex
	}

	CancelErrFunc func(err error)
)

var _ ctx.Context = (*cancelErrCtx)(nil)

// WithCancelError allows creating a context with cancel custom error. The CancelErrFunc
// must be always called when the context is not used anymore
func WithCancelError(parent ctx.Context) (ctx.Context, CancelErrFunc) {
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	c := newCancelErrorCtx(parent)
	// watchdog
	go func() {
		select {
		case <-parent.Done():
			c.cancel(parent.Err())
		case <-c.ch:
		}
	}()
	return c, func(err error) { c.cancel(err) }
}

func newCancelErrorCtx(p ctx.Context) *cancelErrCtx {
	return &cancelErrCtx{
		pCtx: p,
		ch:   make(chan struct{}),
	}
}

func (c *cancelErrCtx) Deadline() (deadline time.Time, ok bool) {
	return c.pCtx.Deadline()
}

func (c *cancelErrCtx) Done() <-chan struct{} {
	return c.ch
}

func (c *cancelErrCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *cancelErrCtx) Value(key any) any {
	return c.pCtx.Value(key)
}

func (c *cancelErrCtx) cancel(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.ch:
		// already closed
		return
	default:
	}

	if c.err == nil {
		c.err = err
		if err == nil {
			c.err = errors2.ErrClosed
		}
	}
	close(c.ch)
}
