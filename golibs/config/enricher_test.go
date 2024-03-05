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
package config

import (
	"github.com/solarisdb/solaris/golibs/logging"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/stretchr/testify/assert"
)

type testB struct {
	IntB    int `json:"ttt"`
	IntBPtr *int
}

type testA struct {
	Field     int
	FieldB    testB
	FieldBPtr *testB
	List      []string
}

type testC struct {
	Str    string
	StrPtr *string
}

func Test_EnricherApplyKeyValues(t *testing.T) {
	logging.SetLevel(logging.TRACE)
	e := newEnricher(testA{})
	e.ApplyKeyValues("teST", "_", map[string]string{"test_list": `["aa", "bb"]`, "TEST_FieldBPtr_ttt": "23", "TEST_FieldB_IntB": "33"})
	assert.Equal(t, testA{FieldB: testB{IntB: 33}, FieldBPtr: &testB{IntB: 23}, List: []string{"aa", "bb"}}, e.Value())

	e.ApplyKeyValues("teST", "_", map[string]string{"test_fieldbptr": `{"ttt": 13, "IntBPtr": 22}`})
	assert.Equal(t, &testB{IntB: 13, IntBPtr: cast.Ptr(22)}, e.Value().FieldBPtr)

	e.ApplyKeyValues("", "_", map[string]string{"fieldbptr_ttt": "42"})
	assert.Equal(t, 42, e.Value().FieldBPtr.IntB)

	// it skips unknown empty field
	oldValue := e.Value()
	e.ApplyKeyValues("", "_", map[string]string{"_": "some value"})
	assert.Equal(t, oldValue, e.Value())
}

func TestApplyOther(t *testing.T) {
	logging.SetLevel(logging.TRACE)
	a := testA{FieldBPtr: &testB{IntB: 1233}, FieldB: testB{IntB: 12}}
	b := testA{FieldBPtr: &testB{IntBPtr: cast.Ptr(10)}, FieldB: testB{IntB: 22}, List: []string{"aa", "bbb"}}
	ea := newEnricher(a)
	eb := newEnricher(b)
	assert.Nil(t, ea.ApplyOther(eb))
	assert.Equal(t, 10, *ea.Value().FieldBPtr.IntBPtr)
	assert.Equal(t, 1233, ea.Value().FieldBPtr.IntB)
	assert.Equal(t, 22, ea.Value().FieldB.IntB)
	assert.Equal(t, eb.Value().List, ea.Value().List)
}

func Test_getAlias(t *testing.T) {
	assert.Equal(t, "", getAlias("lala"))
	assert.Equal(t, "", getAlias(`yaml:"test"`))
	assert.Equal(t, "TEST", getAlias(`json:"test"`))
	assert.Equal(t, "", getAlias(`json:",test"`))
	assert.Equal(t, "-", getAlias(`json:"-,omitempty"`))
	assert.Equal(t, "", getAlias(`json:",omitempty"`))
}

func TestEnricher_LoadFromFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "badTest")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	fn := filepath.Join(dir, "bad.yaml")
	createFile(fn, `sdfkjlafj aldskfjalfdj`)

	logging.SetLevel(logging.TRACE)
	ea := newEnricher(testA{})
	assert.NotNil(t, ea.LoadFromFile(fn))

	fn = filepath.Join(dir, "goodButEmpty.yaml")
	createFile(fn, `some: 1234`)
	assert.Nil(t, ea.LoadFromFile(fn))
	assert.Equal(t, testA{}, ea.Value())

	fn = filepath.Join(dir, "goodButEmpty2.yaml")
	createFile(fn, `
fieldb:
    ttt: 2`)
	assert.Nil(t, ea.LoadFromFile(fn))
	assert.Equal(t, testA{FieldB: testB{IntB: 2}}, ea.Value())

	fn = filepath.Join(dir, "good2.json")
	createFile(fn, `{"fieldb": {"ttt": 22}}`)
	assert.Nil(t, ea.LoadFromFile(fn))
	assert.Equal(t, testA{FieldB: testB{IntB: 22}}, ea.Value())
}

func TestEnricher_LoadFromJSONFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "badTest")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	fn := filepath.Join(dir, "bad.yaml")
	createFile(fn, `sdfkjlafj aldskfjalfdj`)
	logging.SetLevel(logging.TRACE)
	ea := newEnricher(testA{})
	assert.NotNil(t, ea.LoadFromJSONFile(fn))

	fn = filepath.Join(dir, "goodYamlButnotJSON.json")
	createFile(fn, `
fieldb:
    ttt: 2`)
	assert.NotNil(t, ea.LoadFromJSONFile(fn))

	fn = filepath.Join(dir, "good2.yaml")
	createFile(fn, `{"fieldb": {"ttt": 22}}`)
	assert.Nil(t, ea.LoadFromJSONFile(fn))
	assert.Equal(t, testA{FieldB: testB{IntB: 22}}, ea.Value())
}

func TestEnricher_LoadFromYAMLFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "badTest")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	fn := filepath.Join(dir, "bad.yaml")
	createFile(fn, `sdfkjlafj aldskfjalfdj`)
	logging.SetLevel(logging.TRACE)
	ea := newEnricher(testA{})
	assert.NotNil(t, ea.LoadFromYAMLFile(fn))

	fn = filepath.Join(dir, "goodYam.json")
	createFile(fn, `
fieldb:
    ttt: 2`)
	assert.Nil(t, ea.LoadFromYAMLFile(fn))
	assert.Equal(t, testA{FieldB: testB{IntB: 2}}, ea.Value())

	fn = filepath.Join(dir, "goodjsonButnotyaml.yaml")
	createFile(fn, `??{"fieldb": {"ttt": 22}}`)
	assert.NotNil(t, ea.LoadFromYAMLFile(fn))
}

func Test_setfieldValue(t *testing.T) {
	var b testB
	val := reflect.ValueOf(&b)
	f := val.Elem().Field(0)
	assert.Nil(t, setFieldValueByString(f, "123"))
	assert.Equal(t, 123, b.IntB)

	val = reflect.ValueOf(&b)
	f = val.Elem().Field(1)
	assert.Nil(t, setFieldValueByString(f, "23"))
	assert.Equal(t, 23, *b.IntBPtr)

	var c testC
	val = reflect.ValueOf(&c)
	f = val.Elem().Field(0)
	assert.Nil(t, setFieldValueByString(f, "str"))
	assert.Equal(t, "str", c.Str)

	val = reflect.ValueOf(&c)
	f = val.Elem().Field(1)
	assert.Nil(t, setFieldValueByString(f, "str"))
	assert.Equal(t, "str", *c.StrPtr)
}

func Test_isQuoted(t *testing.T) {
	assert.False(t, isQuoted("       "))
	assert.False(t, isQuoted(""))
	assert.False(t, isQuoted("\"asdfa"))
	assert.False(t, isQuoted("asdfasd\""))
	assert.False(t, isQuoted("\""))
	assert.True(t, isQuoted("\"\""))
	assert.True(t, isQuoted("\"asdf\""))
	assert.True(t, isQuoted("   \"asdfasdf\"asdf\" "))
}

func Test_LoadSecrets(t *testing.T) {
	dir, err := ioutil.TempDir("", "badTest")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "bad_secret")
	createFile(path, `sdfkjlafj aldskfjalfdj`)
	enricher := newEnricher(testA{})
	assert.Error(t, LoadJSONAndApply[testA](enricher, path))

	path = filepath.Join(dir, "good_secret")
	createFile(path, `{"FIELD": "1", "FIELDB_TTT": "2", "LIST": "[\"bar\", \"baz\"]"}`)
	assert.NoError(t, LoadJSONAndApply[testA](enricher, path))
	assert.Equal(t, testA{Field: 1, FieldB: testB{IntB: 2}, List: []string{"bar", "baz"}}, enricher.Value())
}

func createFile(name, data string) {
	f, _ := os.Create(name)
	f.WriteString(data)
	f.Close()
}
