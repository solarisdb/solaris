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
	"context"
	"fmt"
	errors2 "github.com/solarisdb/solaris/golibs/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWithCancelError(t *testing.T) {
	assert.Panics(t, func() {
		WithCancelError(nil)
	})

	ctx, cf := WithCancelError(context.Background())
	assert.Nil(t, ctx.Err())
	select {
	case <-ctx.Done():
		t.Fatal("must not happen")
	default:
	}
	err := fmt.Errorf("ta ta")
	cf(err)
	assert.Equal(t, err, ctx.Err())
	_, ok := <-ctx.Done()
	assert.False(t, ok)

	ctx, cf = WithCancelError(context.Background())
	cf(nil)
	assert.Equal(t, errors2.ErrClosed, ctx.Err())

	ctx, cancel := context.WithCancel(context.Background())
	ctx1, cf1 := WithCancelError(ctx)
	cancel()
	<-ctx1.Done()
	assert.Equal(t, ctx.Err(), ctx1.Err())
	cf1(fmt.Errorf("doesn't matter"))
	assert.Equal(t, ctx.Err(), ctx1.Err())

	// new with cancelled
	ctx1, _ = WithCancelError(ctx)
	<-ctx1.Done()
	assert.Equal(t, ctx.Err(), ctx1.Err())
}

func TestCancelErrCtx_Deadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx1, cf1 := WithCancelError(ctx)
	defer cf1(nil)

	tt, ok := ctx.Deadline()
	tt1, ok1 := ctx1.Deadline()
	assert.Equal(t, tt, tt1)
	assert.Equal(t, ok, ok1)
}

func TestCancelErrCtx_Value(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := context.WithValue(ctx, "aa", "bb")
	ctx1, cf1 := WithCancelError(c)
	defer cf1(nil)

	assert.Equal(t, "bb", ctx1.Value("aa"))
	cf1(nil)
	assert.Equal(t, "bb", ctx1.Value("aa"))
	cancel()
	assert.Equal(t, "bb", ctx1.Value("aa"))
}

func TestWithCancelErrorDescenders(t *testing.T) {
	ctx1, cf1 := WithCancelError(context.Background())
	defer cf1(nil)

	childCtx, cancel := context.WithCancel(ctx1)
	defer cancel()
	cf1(nil)
	<-childCtx.Done()
	assert.NotNil(t, childCtx.Err())
}
