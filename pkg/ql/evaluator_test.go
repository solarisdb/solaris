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

package ql

import (
	"fmt"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

type testRecord struct {
	StringField string
	TimeField   time.Time
}

var (
	testDialect = Dialect[testRecord]{
		StringParamID: { // strings are rvalues only
			Flags: PfRValue | PfComparable | PfConstValue,
			ValueF: func(p *Param, _ testRecord) (any, error) {
				return p.Const.Value(), nil
			},
			Type: VTString,
		},
		ArrayParamID: { // arrays are rvalues only
			Flags: PfRValue | PfConstValue,
			ValueF: func(p *Param, _ testRecord) (any, error) {
				var strArr []string
				for _, elem := range p.Array {
					strArr = append(strArr, elem.Value())
				}
				return strArr, nil
			},
			Type: VTStrings,
		},
		"StringField": {
			Flags: PfLValue | PfComparable | PfInLike,
			ValueF: func(p *Param, r testRecord) (any, error) {
				return r.StringField, nil
			},
			Type: VTString,
		},
		"TimeField": {
			Flags: PfLValue | PfComparable,
			ValueF: func(p *Param, r testRecord) (any, error) {
				return r.TimeField, nil
			},
			Type: VTTime,
		},
		"ErrValue": {
			Flags: PfLValue | PfComparable,
			ValueF: func(p *Param, r testRecord) (any, error) {
				return r.TimeField, fmt.Errorf("ValueF error")
			},
			Type: VTString,
		},
		"ErrCheck": {
			Flags: PfLValue | PfComparable,
			CheckF: func(p *Param) error {
				return errors.ErrInvalid
			},
			ValueF: func(p *Param, r testRecord) (any, error) {
				return r.TimeField, fmt.Errorf("ValueF error")
			},
			Type: VTTime,
		},
	}
)

func BenchmarkEvaluateSimple(b *testing.B) {
	expr, _ := Parse("TimeField < '2022-11-11 12:34:53'")
	f, _ := BuildExprF(expr, testDialect)

	r := testRecord{}
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		f(r)
	}
}

func Test_like(t *testing.T) {
	assert.True(t, like("abc", "%", '%'))
	assert.True(t, like("abc", "%bc", '%'))
	assert.True(t, like("abcd", "ab%", '%'))
	assert.True(t, like("abvcefd", "a%c%d", '%'))
	assert.True(t, like("abvacefd", "%ac%", '%'))
	assert.False(t, like("abc", "%d", '%'))
	assert.False(t, like("abvccefd", "a%ac%d", '%'))
}

func TestLogCondEval_EvalTrue(t *testing.T) {
	expr, err := Parse("tag('tag1') = 'val1' OR tag('tag2') IN ['val22'] OR tag('tag3') like '%val333%'")
	assert.Nil(t, err)
	eval, err := BuildExprF(expr, LogsCondDialect)
	assert.Nil(t, err)

	for i := 1; i < 5; i++ {
		log := &solaris.Log{ID: ulidutils.NewID(), Tags: map[string]string{
			"tag1": fmt.Sprintf("val%s", strings.Repeat("1", i)),
			"tag2": fmt.Sprintf("val%s", strings.Repeat("2", i)),
			"tag3": fmt.Sprintf("val%s", strings.Repeat("3", i)),
		}}
		assert.True(t, eval(log))
	}
}

func TestLogCondEval_EvalFalse(t *testing.T) {
	expr, err := Parse("tag('tag1') like '%v%' AND NOT tag('tag2') IN ['val2']")
	assert.Nil(t, err)
	eval, err := BuildExprF(expr, LogsCondDialect)
	assert.Nil(t, err)

	log1 := &solaris.Log{ID: ulidutils.NewID(), Tags: map[string]string{
		"tag1": "val1",
		"tag2": "val2",
		"tag3": "val3",
	}}
	log2 := &solaris.Log{ID: ulidutils.NewID(), Tags: map[string]string{
		"tag1": "al11",
		"tag2": "val22",
		"tag3": "val33",
	}}

	assert.False(t, eval(log1))
	assert.False(t, eval(log2))
}

func TestBuildExprF(t *testing.T) {
	f, err := BuildExprF(nil, testDialect)
	assert.Nil(t, err)
	assert.True(t, f(testRecord{}))

	expr, err := Parse("")
	assert.Nil(t, err)
	f, err = BuildExprF(expr, testDialect)
	assert.Nil(t, err)
	assert.True(t, f(testRecord{}))

	expr, err = Parse("TimeField < '2022-11-11 12:34:53'")
	assert.Nil(t, err)
	f, err = BuildExprF(expr, testDialect)
	assert.Nil(t, err)
	assert.True(t, f(testRecord{}))
	assert.False(t, f(testRecord{TimeField: time.Now()}))

	expr, err = Parse("ErrCheck < '2022-11-11 12:34:53'")
	assert.Nil(t, err)
	_, err = BuildExprF(expr, testDialect)
	assert.NotNil(t, err)

	expr, err = Parse("ErrValue < '2022-11-11 12:34:53'")
	assert.Nil(t, err)
	f, err = BuildExprF(expr, testDialect)
	assert.False(t, f(testRecord{}))
}
