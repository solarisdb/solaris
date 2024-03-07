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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseParam(t *testing.T) {
	expr, err := Parse("1234")
	assert.Nil(t, err)

	cond := expr.Or[0].And[0].Cond
	assert.Equal(t, float32(1234.0), cond.FirstParam.Const.Number)
	assert.Equal(t, NumberParamID, cond.FirstParam.id())

	expr, err = Parse("'1234'")
	assert.Nil(t, err)

	cond = expr.Or[0].And[0].Cond
	assert.Equal(t, "1234", cond.FirstParam.Const.String)
	assert.Equal(t, StringParamID, cond.FirstParam.id())

	expr, err = Parse("lala")
	assert.Nil(t, err)

	cond = expr.Or[0].And[0].Cond
	assert.Equal(t, "lala", cond.FirstParam.Identifier)
	assert.Equal(t, "lala", cond.FirstParam.id())

	expr, err = Parse("lala ( )")
	assert.Nil(t, err)

	cond = expr.Or[0].And[0].Cond
	assert.Equal(t, Function{Name: "lala"}, *cond.FirstParam.Function)
	assert.Equal(t, "lala", cond.FirstParam.id())

	expr, err = Parse("lala ( 1234)")
	assert.Nil(t, err)

	cond = expr.Or[0].And[0].Cond
	assert.Equal(t, Function{Name: "lala", Params: []*Param{{Const: &Const{Number: float32(1234)}}}}, *cond.FirstParam.Function)
	assert.Equal(t, "lala", cond.FirstParam.id())

	_, err = Parse("lala ( 1234,hhh)")
	assert.Nil(t, err)
	_, err = Parse("[1234, 22]")
	assert.Nil(t, err)
}

func TestParseCondition(t *testing.T) {
	expr, err := Parse("1234")
	assert.Nil(t, err)

	cond := expr.Or[0].And[0].Cond
	assert.Equal(t, float32(1234.0), cond.FirstParam.Const.Number)
	assert.Nil(t, expr.Or[0].And[0].Cond.SecondParam)

	expr, err = Parse("f1() != f2('asdf')")
	assert.Nil(t, err)

	cond = expr.Or[0].And[0].Cond
	assert.Equal(t, "f1", cond.FirstParam.Function.Name)
	assert.Equal(t, "!=", cond.Op)
	assert.Equal(t, "f2", cond.SecondParam.Function.Name)
}

func TestExpressions(t *testing.T) {
	testOk(t, "1234.34")
	testOk(t, "'string'")
	testOk(t, "var2")
	testOk(t, "f()")
	testOk(t, "f(1)")
	testOk(t, "f(1, 2)")
	testOk(t, "1234 != 1234 and f()")
	testOk(t, "1234 != 1234 and (f(1234, var2, f2(34, f1())) or var1 = 'sdf')")
	testOk(t, "f1('abc') in [1,2,3]")
}

func testOk(t *testing.T, e string) {
	_, err := Parse(e)
	assert.Nil(t, err)
}
