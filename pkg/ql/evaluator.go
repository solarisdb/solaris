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
	"github.com/solarisdb/solaris/golibs/container"
	"github.com/solarisdb/solaris/golibs/errors"
	"slices"
	"strings"
	"time"
)

type (
	// ExprF represet the Expression function to evaluate the expression for the type T
	ExprF[T any] func(t T) bool

	exprBuilder[T any] struct {
		f       ExprF[T]
		dialect Dialect[T]
	}
)

func positive[T any](_ T) bool { return true }
func negative[T any](_ T) bool { return false }

// BuildExprF allows to build the ExprF[T] function by the expression and the dialect provided. The result
// function may be used for testing a value of T either it matches the expression or not.
func BuildExprF[T any](expr *Expression, dialect Dialect[T]) (ExprF[T], error) {
	if expr == nil {
		return positive[T], nil
	}
	var eb exprBuilder[T]
	eb.dialect = dialect
	if err := eb.buildOrConds(expr.Or); err != nil {
		return nil, err
	}
	return eb.f, nil
}

func (eb *exprBuilder[T]) buildOrConds(ocn []*OrCondition) error {
	if len(ocn) == 0 {
		eb.f = positive[T]
		return nil
	}

	err := eb.buildXConds(ocn[0].And)
	if err != nil {
		return err
	}

	if len(ocn) == 1 {
		// no need to go ahead anymore
		return nil
	}

	efd0 := eb.f
	err = eb.buildOrConds(ocn[1:])
	if err != nil {
		return err
	}
	efd1 := eb.f

	eb.f = func(t T) bool { return efd0(t) || efd1(t) }
	return nil
}

func (eb *exprBuilder[T]) buildXConds(cn []*XCondition) error {
	if len(cn) == 0 {
		eb.f = positive[T]
		return nil
	}

	if len(cn) == 1 {
		return eb.buildXCond(cn[0])
	}

	if err := eb.buildXCond(cn[0]); err != nil {
		return err
	}

	efd0 := eb.f
	if err := eb.buildXConds(cn[1:]); err != nil {
		return err
	}
	efd1 := eb.f

	eb.f = func(t T) bool { return efd0(t) && efd1(t) }
	return nil
}

func (eb *exprBuilder[T]) buildXCond(xc *XCondition) (err error) {
	if xc.Expr != nil {
		err = eb.buildOrConds(xc.Expr.Or)
	} else {
		err = eb.buildCond(xc.Cond)
	}

	if err != nil {
		return err
	}

	if xc.Not {
		efd1 := eb.f
		eb.f = func(t T) bool { return !efd1(t) }
	}

	return nil
}

func (eb *exprBuilder[T]) buildCond(cn *Condition) (err error) {
	d, ok := eb.dialect[cn.FirstParam.ID()]
	if !ok {
		return fmt.Errorf("unknown parameter %s: %w", cn.FirstParam.Name(false), errors.ErrInvalid)
	}
	if d.Flags&PfLValue == 0 {
		return fmt.Errorf("parameter %s cannot be on the left side of the condition: %w", cn.FirstParam.Name(false), errors.ErrInvalid)
	}
	p1 := &cn.FirstParam
	if err := d.check(p1); err != nil {
		return err
	}

	p1vf, err := eb.paramDialect2ValueF(d, p1, nil)
	if err != nil {
		return err
	}
	if cn.Op == "" {
		if d.Flags&PfNop == 0 {
			return fmt.Errorf("parameter %s should be compared with something in a condition: %w", p1.Name(false), errors.ErrInvalid)
		}
		if d.Type != VTBool {
			return fmt.Errorf("parameter %s should be or return bool value in a condition: %w", p1.Name(false), errors.ErrInvalid)
		}
		eb.f = func(t T) bool {
			b, err := p1vf(nil, t)
			if err != nil {
				return false
			}
			return b.(bool)
		}
		return nil
	}
	if d.Flags&PfNop != 0 {
		return fmt.Errorf("parameter %s cannot be compared (%s) in the condition: %w", cn.FirstParam.Name(false), cn.Op, errors.ErrInvalid)
	}

	p2 := cn.SecondParam
	if p2 == nil {
		return fmt.Errorf("wrong condition for the param %s and the operation %q - no second parameter: %w", p2.Name(false), cn.Op, errors.ErrInvalid)
	}
	d2, ok := eb.dialect[p2.ID()]
	if !ok {
		return fmt.Errorf("unknown second parameter %s: %w", p2.Name(false), errors.ErrInvalid)
	}
	if d2.Flags&PfRValue == 0 {
		return fmt.Errorf("parameter %s cannot be on the right side of the condition: %w", p2.Name(false), errors.ErrInvalid)
	}
	if d2.Flags&PfNop != 0 {
		return fmt.Errorf("parameter %s cannot be compared (%s) in the condition: %w", p2.Name(false), cn.Op, errors.ErrInvalid)
	}

	op := strings.ToUpper(cn.Op)
	switch op {
	case "<", ">":
		if d.Flags&PfComparable == 0 && d.Flags&PfGreaterLess == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the operation %s: %w", p1.Name(false), cn.Op, errors.ErrInvalid)
		}
		if d2.Flags&PfComparable == 0 && d2.Flags&PfGreaterLess == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the operation %s: %w", p2.Name(false), cn.Op, errors.ErrInvalid)
		}
		p2vf, err := eb.paramDialect2ValueF(d2, p2, &d.Type)
		if err != nil {
			return err
		}
		return eb.compare(p1vf, p2vf, d.Type, op)
	case "<=", ">=", "!=", "=":
		if d.Flags&PfComparable == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the operation %s: %w", p1.Name(false), cn.Op, errors.ErrInvalid)
		}
		if d2.Flags&PfComparable == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the operation %s: %w", p2.Name(false), cn.Op, errors.ErrInvalid)
		}
		p2vf, err := eb.paramDialect2ValueF(d2, p2, &d.Type)
		if err != nil {
			return err
		}
		return eb.compare(p1vf, p2vf, d.Type, op)
	case "IN":
		if d.Flags&PfInLike == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the IN : %w", p1.Name(false), errors.ErrInvalid)
		}
		if p2.ID() != ArrayParamID {
			return fmt.Errorf("the second parameter %s must be an array: %w", p2.Name(false), errors.ErrInvalid)
		}
		arr, err := d2.ValueF(p2, *new(T))
		if err != nil {
			return err
		}
		return eb.in(p1vf, arr.([]string))
	case "LIKE":
		if d.Flags&PfInLike == 0 {
			return fmt.Errorf("the first parameter %s is not applicable for the LIKE : %w", p1.Name(false), errors.ErrInvalid)
		}
		if p2.ID() != StringParamID {
			return fmt.Errorf("the right value(%s) of LIKE must be a string: %w", p1.Name(false), errors.ErrInvalid)
		}
		str, err := d2.ValueF(p2, *new(T))
		if err != nil {
			return err
		}
		return eb.like(p1vf, str.(string))
	default:
		return fmt.Errorf("unknown operation %s: %w", cn.Op, errors.ErrInvalid)
	}
	panic("unreacheable")
}

// compare builds the ExprF, which will build comparison of vf1 and vf2 results depending on the op
func (eb *exprBuilder[T]) compare(vf1, vf2 valueF[T], tp ValueType, op string) error {
	switch tp {
	case VTTime:
		switch op {
		case "<":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(time.Time).Before(v2.(time.Time))
			}
		case ">":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(time.Time).After(v2.(time.Time))
			}
		default:
			return fmt.Errorf("unsupport operation %s for the time comparision: %w", op, errors.ErrInvalid)
		}
	case VTString:
		switch op {
		case "<":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(string) < v2.(string)
			}
		case ">":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(string) > v2.(string)
			}
		case ">=":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(string) >= v2.(string)
			}
		case "<=":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(string) <= v2.(string)
			}
		case "=":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(string) == v2.(string)
			}
		case "!=":
			eb.f = func(t T) bool {
				v1, err := vf1(nil, t)
				if err != nil {
					return false
				}
				v2, err := vf2(nil, t)
				if err != nil {
					return false
				}
				return v1.(string) != v2.(string)
			}
		default:
			return fmt.Errorf("unsupport operation %s for the string comparision: %w", op, errors.ErrInvalid)
		}
	}
	return nil
}

// in create the IN operation in eb.f
func (eb *exprBuilder[T]) in(vf valueF[T], arr []string) error {
	if len(arr) == 0 {
		eb.f = negative[T]
		return nil
	}
	eb.f = func(t T) bool {
		v, err := vf(nil, t)
		if err != nil {
			return false
		}
		return slices.Contains(arr, v.(string))
	}
	return nil
}

// like creates the LIKE operation in eb.f
func (eb *exprBuilder[T]) like(vf valueF[T], pat string) error {
	if pat == "" {
		eb.f = func(t T) bool {
			s, err := vf(nil, t)
			if err != nil {
				return false
			}
			return s.(string) == ""
		}
		return nil
	}

	if pat == "%" {
		eb.f = positive[T]
		return nil
	}

	eb.f = func(t T) bool {
		s, err := vf(nil, t)
		if err != nil {
			return false
		}
		return like(s.(string), pat, '%')
	}
	return nil
}

func like(s, pat string, wildcard byte) bool {
	prev := make([]bool, len(s)+1)
	next := make([]bool, len(s)+1)
	prev[0] = true

	for i := 0; i < len(pat); i++ {
		container.SliceFill(next, false)
		if pat[i] == wildcard {
			next[0] = prev[0]
		}
		for j := 0; j < len(s); j++ {
			if pat[i] == wildcard {
				next[j+1] = prev[j] || prev[j+1] || next[j]
			} else if pat[i] == s[j] {
				next[j+1] = prev[j]
			}
		}
		prev, next = next, prev
	}
	return prev[len(s)]
}

// paramDialect2ValueF gets the param p and turns it to the valueF function, which will return the type vt. If the vt is nil
// the function will return the type which is declared in d
func (eb *exprBuilder[T]) paramDialect2ValueF(d ParamDialect[T], p *Param, vt *ValueType) (valueF[T], error) {
	var res valueF[T]
	var v T
	if d.Flags&PfConstValue == 0 {
		// the function depends on t, so we need to wrap it with calling it with the external p reference
		res = func(_ *Param, t T) (any, error) {
			// note that the p is ref to the paramDialect2ValueF() argument!
			v, err := d.ValueF(p, t)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
		if vt != nil {
			return castValueF(res, d.Type, *vt)
		}
		return res, nil
	}

	// ok, the result is a constant, so we may calculate it right now
	var constVal any
	var err error
	if vt != nil {
		casted, err1 := castValueF(d.ValueF, d.Type, *vt)
		if err1 != nil {
			return nil, err1
		}
		constVal, err = casted(p, v)
	} else {
		constVal, err = d.ValueF(p, v)
	}

	if err != nil {
		return nil, err
	}
	return func(p *Param, t T) (any, error) {
		return constVal, nil
	}, nil
}

// castValueF wraps the function f to the function valueF[T], which will cast the return type of f from "from" to "to"
func castValueF[T any](f valueF[T], from, to ValueType) (valueF[T], error) {
	if from == to {
		return f, nil
	}
	switch from {
	case VTString:
		switch to {
		case VTTime:
			return func(p *Param, t T) (any, error) {
				s, err := f(p, t)
				if err != nil {
					return s, err
				}
				return parseDateTime(s.(string))
			}, nil
		}
	}
	return f, fmt.Errorf("could not cast value of type %s to %s: %w", typeNames[from], typeNames[to], errors.ErrInvalid)
}
