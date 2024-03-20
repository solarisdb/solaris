package sss

import (
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
	err := ss.Put("aaa/", strings.NewReader(data1))
	assert.NotNil(t, err)

	// populate
	err = ss.Put("/val1", strings.NewReader(data1))
	assert.Nil(t, err)
	err = ss.Put("/val2", strings.NewReader(data1))
	assert.Nil(t, err)
	err = ss.Put("/val2", strings.NewReader(data2))
	assert.Nil(t, err)

	err = ss.Put("/subfldr/val3", strings.NewReader(data1))
	assert.Nil(t, err)
	err = ss.Put("/subfldr/val4", strings.NewReader(data1))
	assert.Nil(t, err)

	err = ss.Put("/subfldr/subfldr2/val4", strings.NewReader(data1))
	assert.Nil(t, err)

	// wrong path
	_, err = ss.List("")
	assert.NotNil(t, err)

	res, err := ss.List("/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/subfldr/", "/val1", "/val2"}, res)

	res, err = ss.List("/subfldr/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/subfldr/subfldr2/", "/subfldr/val3", "/subfldr/val4"}, res)

	r, err := ss.Get("/val2")
	assert.Nil(t, err)
	buf, _ := ioutil.ReadAll(r)
	assert.Equal(t, data2, string(buf))

	r, err = ss.Get("/subfldr/val3")
	assert.Nil(t, err)
	buf, _ = ioutil.ReadAll(r)
	assert.Equal(t, data1, string(buf))

	_, err = ss.Get("/val3")
	assert.NotNil(t, err)
	assert.Equal(t, errors.ErrNotExist, err)

	// deleting
	ss.Delete("/subfldr/val3")
	ss.Delete("/subfldr/val3")
	res, err = ss.List("/subfldr/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/subfldr/subfldr2/", "/subfldr/val4"}, res)

	ss.Delete("/subfldr/subfldr2/val4")

	ss.Delete("/subfldr/val4")
	res, err = ss.List("/subfldr/")
	assert.Nil(t, err)
	assert.Equal(t, []string{}, res)

	res, err = ss.List("/")
	assert.Nil(t, err)
	sort.Strings(res)
	assert.Equal(t, []string{"/val1", "/val2"}, res)

	ss.Delete("/val1")
	ss.Delete("/val2")
	res, err = ss.List("/")
	assert.Nil(t, err)
	assert.Equal(t, []string{}, res)
}
