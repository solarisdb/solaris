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
)

const (
	// PfLValue the parameter can be a lvalue in the condition
	PfLValue = 1 << 0
	// PfRValue the parameter can be a rvalue in the condition
	PfRValue = 1 << 1
	// PfNop the parameter cannot have any operation
	PfNop = 1 << 2
	// PfComparable the parameter can be compared: <, >, !=, =, >=, <=
	PfComparable = 1 << 3
	// PfInLike the IN or LIKE operations are allowed for the param
	PfInLike = 1 << 4
	// PfGreaterLess the parameter can be compared with < or > only
	PfGreaterLess = 1 << 5
	// The ParamDialect ValueF() is a constant value and should not be re-calculated every time when called
	// it also means that ValueF will return a value for the *new(T)
	PfConstValue = 1 << 6
)

type (
	// ValueType defines the type returned by a parameter value function (see below)
	ValueType int

	// ParamDialect is used for describing grama objects in the dialect
	ParamDialect[T any] struct {
		// Flags contains flags associated with the parameter, please see Pf constants above
		Flags int
		// CheckF is used for calculating the parameter value while building an evaluator function
		// If not specified, then the parameter is always correct
		CheckF func(p *Param) error
		// ValueF is the function which allows to get the parameter value. The function MUST NOT be called
		// if the CheckF returns an error
		ValueF valueF[T]
		// Define the type for the ValueF result
		Type ValueType
	}

	valueF[T any]  func(p *Param, t T) (any, error)
	Dialect[T any] map[string]ParamDialect[T]
)

const (
	VTNA      ValueType = 0
	VTString  ValueType = 1
	VTTime    ValueType = 2
	VTBool    ValueType = 3
	VTStrings ValueType = 4
)

var typeNames = []string{"unknown", "string", "time", "bool", "strings"}

var (
	LogsCondDialect = Dialect[*solaris.Log]{
		StringParamID: { // strings are rvalues only
			Flags: PfRValue | PfComparable | PfConstValue,
			ValueF: func(p *Param, _ *solaris.Log) (any, error) {
				return p.Const.Value(), nil
			},
			Type: VTString,
		},
		ArrayParamID: { // arrays are rvalues only
			Flags: PfRValue | PfConstValue,
			ValueF: func(p *Param, _ *solaris.Log) (any, error) {
				var strArr []string
				for _, elem := range p.Array {
					strArr = append(strArr, elem.Value())
				}
				return strArr, nil
			},
			Type: VTStrings,
		},
		"logID": {
			Flags: PfLValue | PfComparable | PfInLike,
			ValueF: func(p *Param, log *solaris.Log) (any, error) {
				return log.ID, nil
			},
			Type: VTString,
		},
		"tag": { // tag function is written the way -> 'tag("abc") in ["1", "2", "3"]' or 'tag("t1") = "aaa"'
			Flags: PfLValue | PfComparable | PfRValue | PfInLike,
			CheckF: func(p *Param) error {
				if p.Function == nil {
					return fmt.Errorf("tag must be a function: %w", errors.ErrInvalid)
				}
				if len(p.Function.Params) != 1 {
					return fmt.Errorf("tag() function expects only one parameter - the name of the tag: %w", errors.ErrInvalid)
				}
				if p.Function.Params[0].ID() != StringParamID {
					return fmt.Errorf("tag() function expects the tag name (string) as the parameter: %w", errors.ErrInvalid)
				}
				return nil
			},
			ValueF: func(p *Param, log *solaris.Log) (any, error) {
				if len(log.Tags) == 0 {
					return "", nil
				}
				return log.Tags[p.Function.Params[0].Name(true)], nil
			},
			Type: VTString,
		},
	}
)

// check returns whether the parameter is ok or not. The function is used by the evaluator
func (pd ParamDialect[T]) check(p *Param) error {
	if pd.CheckF != nil {
		return pd.CheckF(p)
	}
	return nil
}
