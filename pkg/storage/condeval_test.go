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
package storage

import (
	"fmt"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/ql"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestLogCondEval_EvalTrue(t *testing.T) {
	expr, err := ql.Parse("tag('tag1') = 'val1' OR tag('tag2') IN ['val22'] OR tag('tag3') like '%val333%'")
	assert.Nil(t, err)
	eval := LogsCondEval

	var ok bool
	var log *solaris.Log
	for i := 1; i < 5; i++ {
		log = &solaris.Log{ID: ulidutils.NewID(), Tags: map[string]string{
			"tag1": fmt.Sprintf("val%s", strings.Repeat("1", i)),
			"tag2": fmt.Sprintf("val%s", strings.Repeat("2", i)),
			"tag3": fmt.Sprintf("val%s", strings.Repeat("3", i)),
		}}
		ok, err = eval.Eval(log, expr)
		assert.Nil(t, err)
		assert.True(t, ok)
	}
}

func TestLogCondEval_EvalFalse(t *testing.T) {
	expr, err := ql.Parse("tag('tag1') like '%v%' AND NOT tag('tag2') IN ['val2']")
	assert.Nil(t, err)
	eval := LogsCondEval

	log1 := &solaris.Log{ID: ulidutils.NewID(), Tags: map[string]string{
		"tag1": fmt.Sprintf("val1"),
		"tag2": fmt.Sprintf("val2"),
		"tag3": fmt.Sprintf("val3"),
	}}
	log2 := &solaris.Log{ID: ulidutils.NewID(), Tags: map[string]string{
		"tag1": fmt.Sprintf("val11"),
		"tag2": fmt.Sprintf("val22"),
		"tag3": fmt.Sprintf("val33"),
	}}

	ok, err := eval.Eval(log1, expr)
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = eval.Eval(log2, expr)
	assert.Nil(t, err)
	assert.True(t, ok)
}

func Test_like(t *testing.T) {
	assert.True(t, LogsCondEval.Like("abc", "%"))
	assert.True(t, LogsCondEval.Like("abc", "%bc"))
	assert.True(t, LogsCondEval.Like("abcd", "ab%"))
	assert.True(t, LogsCondEval.Like("abvcefd", "a%c%d"))
	assert.True(t, LogsCondEval.Like("abvacefd", "%ac%"))
	assert.False(t, LogsCondEval.Like("abc", "%d"))
	assert.False(t, LogsCondEval.Like("abvccefd", "a%ac%d"))
}
