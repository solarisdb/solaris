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

package sss

import (
	"context"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"sort"
	"strings"
	"testing"
)

// TestSimpleStorage a bunch of tests run against a kvs instance
func TestSimpleStorage(t *testing.T, ss Storage) {
	data1 := "value"
	data2 := "value2"

	// wrong key
	err := ss.Put(context.Background(), "aaa/", strings.NewReader(data1))
	assert.NotNil(t, err)

	// populate
	err = ss.Put(context.Background(), "/val1", strings.NewReader(data1))
	assert.Nil(t, err)
	err = ss.Put(context.Background(), "/val2", strings.NewReader(data1))
	assert.Nil(t, err)
	err = ss.Put(context.Background(), "/val2", strings.NewReader(data2))
	assert.Nil(t, err)

	err = ss.Put(context.Background(), "/subfldr/val3", strings.NewReader(data1))
	assert.Nil(t, err)
	err = ss.Put(context.Background(), "/subfldr/val4", strings.NewReader(data1))
	assert.Nil(t, err)

	err = ss.Put(context.Background(), "/subfldr/subfldr2/val4", strings.NewReader(data1))
	assert.Nil(t, err)

	// wrong path
	_, err = ss.List(context.Background(), "")
	assert.NotNil(t, err)

	res, err := ss.List(context.Background(), "/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/subfldr/", "/val1", "/val2"}, res)

	res, err = ss.List(context.Background(), "/subfldr/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/subfldr/subfldr2/", "/subfldr/val3", "/subfldr/val4"}, res)

	r, err := ss.Get(context.Background(), "/val2")
	assert.Nil(t, err)
	buf, _ := ioutil.ReadAll(r)
	assert.Equal(t, data2, string(buf))

	r, err = ss.Get(context.Background(), "/subfldr/val3")
	assert.Nil(t, err)
	buf, _ = ioutil.ReadAll(r)
	assert.Equal(t, data1, string(buf))

	_, err = ss.Get(context.Background(), "/val3")
	assert.NotNil(t, err)
	assert.Equal(t, errors.ErrNotExist, err)

	// deleting
	ss.Delete(context.Background(), "/subfldr/val3")
	ss.Delete(context.Background(), "/subfldr/val3")
	res, err = ss.List(context.Background(), "/subfldr/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/subfldr/subfldr2/", "/subfldr/val4"}, res)

	ss.Delete(context.Background(), "/subfldr/subfldr2/val4")

	ss.Delete(context.Background(), "/subfldr/val4")
	res, err = ss.List(context.Background(), "/subfldr/")
	assert.Nil(t, err)
	assert.Equal(t, []string{}, res)

	res, err = ss.List(context.Background(), "/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/val1", "/val2"}, res)

	ss.Delete(context.Background(), "/val1")
	ss.Delete(context.Background(), "/val2")
	res, err = ss.List(context.Background(), "/")
	assert.Nil(t, err)
	assert.Equal(t, []string{}, res)
}
