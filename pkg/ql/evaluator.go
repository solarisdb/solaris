package ql

import (
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"slices"
	"strings"
)

type (
	// Evaluator describes the dialect/grammar of how to interpret
	// the AST tree nodes for an abstract expression with types T
	Evaluator[T any] struct {
		Dialect map[string]ParamDialect[T, any]
		Like    func(s, p string) bool
	}
)

// Eval evaluates the expression expr for a specific T entity
func (e *Evaluator[T]) Eval(v T, expr *Expression) (bool, error) {
	for _, oc := range expr.Or {
		orOk := true
		for _, xc := range oc.And {
			var (
				andOk bool
				err   error
			)
			if xc.Expr != nil {
				andOk, err = e.Eval(v, xc.Expr)
			} else {
				andOk, err = e.eval(v, xc.Cond)
			}
			if err != nil {
				return false, err
			}
			if xc.Not == andOk {
				orOk = false
				break
			}
		}
		if orOk {
			return true, nil
		}
	}
	return false, nil
}

func (e *Evaluator[T]) eval(v T, c *Condition) (bool, error) {
	// first param
	p1, ok := e.Dialect[c.FirstParam.ID()]
	if !ok {
		return false, fmt.Errorf("unknown parameter %q: %w", c.FirstParam.Name(), errors.ErrInvalid)
	}
	if p1.Flags&PfLValue == 0 {
		return false, fmt.Errorf("parameter %q cannot be on the left side of the condition: %w",
			c.FirstParam.Name(), errors.ErrInvalid)
	}
	if c.Op == "" {
		return false, fmt.Errorf("parameter %q should be compared with something in a condition: %w",
			c.FirstParam.Name(), errors.ErrInvalid)
	}
	// second param
	if c.SecondParam == nil {
		return false, fmt.Errorf("wrong condition for the param %q and the operation %q - no second parameter: %w",
			c.FirstParam.Name(), c.Op, errors.ErrInvalid)
	}
	p2, ok := e.Dialect[c.SecondParam.ID()]
	if !ok {
		return false, fmt.Errorf("unknown second parameter %s: %w",
			c.SecondParam.Name(), errors.ErrInvalid)
	}
	if p2.Flags&PfRValue == 0 {
		return false, fmt.Errorf("parameter %q cannot be on the right side of the condition: %w",
			c.SecondParam.Name(), errors.ErrInvalid)
	}
	// param values
	getParamValues := func() (any, any, error) {
		v1, err := p1.Value(&c.FirstParam, v)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get parameter %q value for the operation %q: %w",
				c.FirstParam.Name(), c.Op, errors.ErrInvalid)
		}
		v2, err := p2.Value(c.SecondParam, v)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get parameter %q value for the operation %q: %w",
				c.SecondParam.Name(), c.Op, errors.ErrInvalid)
		}
		return v1, v2, nil
	}

	// eval
	switch op := strings.ToUpper(c.Op); op {
	case "<", ">", "<=", ">=", "!=", "=":
		if p1.Flags&PfComparable == 0 {
			return false, fmt.Errorf("the first parameter %q is not applicable for the operation %q: %w",
				c.FirstParam.Name(), c.Op, errors.ErrInvalid)
		}
		if p2.Flags&PfComparable == 0 {
			return false, fmt.Errorf("the first parameter %s is not applicable for the operation %q: %w",
				c.SecondParam.Name(), c.Op, errors.ErrInvalid)
		}
		v1, v2, err := getParamValues()
		if err != nil {
			return false, err
		}
		return compare(v1.(string), v2.(string), op), nil
	case "IN":
		if p1.Flags&PfInLike == 0 {
			return false, fmt.Errorf("the first parameter %q is not applicable for the IN : %w",
				c.FirstParam.Name(), errors.ErrInvalid)
		}
		if c.SecondParam.ID() != ArrayParamID {
			return false, fmt.Errorf("the second parameter %q must be an array: %w",
				c.SecondParam.Name(), errors.ErrInvalid)
		}
		v1, v2, err := getParamValues()
		if err != nil {
			return false, err
		}
		return in(v1.(string), v2.([]string)), nil
	case "LIKE":
		if p1.Flags&PfInLike == 0 {
			return false, fmt.Errorf("the first parameter %q is not applicable for the LIKE : %w",
				c.FirstParam.Name(), errors.ErrInvalid)
		}
		if c.SecondParam.ID() != StringParamID {
			return false, fmt.Errorf("the right value(%q) of LIKE must be a string: %w",
				c.SecondParam.Name(), errors.ErrInvalid)
		}
		v1, v2, err := getParamValues()
		if err != nil {
			return false, err
		}
		return e.Like(v1.(string), v2.(string)), nil
	}
	return false, fmt.Errorf("unknown operation %q: %w", c.Op, errors.ErrInvalid)
}

func compare(s1, s2, op string) bool {
	switch op {
	case "<":
		return s1 < s2
	case ">":
		return s1 > s2
	case "<=":
		return s1 <= s2
	case ">=":
		return s1 >= s2
	case "!=":
		return s1 != s2
	case "=":
		return s1 == s2
	}
	return false
}

func in(s string, arr []string) bool {
	return slices.Contains(arr, s)
}
