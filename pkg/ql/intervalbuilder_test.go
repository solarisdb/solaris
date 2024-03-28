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
	"github.com/solarisdb/solaris/pkg/intervals"
	"github.com/stretchr/testify/assert"
	"testing"
	"unicode/utf8"
)

var (
	testIntervalDialect = Dialect[testRecord]{
		StringParamID: {
			Flags: PfRValue | PfComparable | PfConstValue,
			ValueF: func(p *Param, _ testRecord) (any, error) {
				return p.Const.Value(), nil
			},
			Type: VTString,
		},
		"t": {
			Flags: PfLValue | PfComparable,
			ValueF: func(p *Param, r testRecord) (any, error) {
				return p.Const.Value(), nil
			},
			Type: VTString,
		},
	}
)

var testIntervalBuilder = NewParamIntervalBuilder(intervals.BasisString, testIntervalDialect, "t", OpsAll)

func TestIntervalBuilder_NoInterval(t *testing.T) {
	expr, err := Parse("(t < 'b' AND t > 'c')")
	assert.Nil(t, err)
	ii, err := testIntervalBuilder.Build(expr)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ii))
}

func TestIntervalBuilder_OneInterval(t *testing.T) {
	expr, err := Parse("(t >= 'b' AND t < 'c')")
	assert.Nil(t, err)
	ii, err := testIntervalBuilder.Build(expr)
	assert.Nil(t, err)
	assert.Nil(t, err)
	assert.True(t, ii[0].IsOpenR())
	assert.Equal(t, "b", ii[0].L)
	assert.Equal(t, "c", ii[0].R)
}

func TestIntervalBuilder_TwoIntervals(t *testing.T) {
	expr, err := Parse("((t > 'a' AND t < 'c') AND (t > 'b' AND t < 'e')) OR (t > 'k')")
	assert.Nil(t, err)
	ii, err := testIntervalBuilder.Build(expr)
	i1, i2 := ii[0], ii[1] // ('b', 'c'), ('k', max]
	assert.Nil(t, err)
	assert.True(t, i1.IsOpen())
	assert.Equal(t, "b", i1.L)
	assert.Equal(t, "c", i1.R)
	assert.True(t, i2.IsOpenL())
	assert.Equal(t, "k", i2.L)
	assert.Equal(t, string(utf8.MaxRune), i2.R)
}
