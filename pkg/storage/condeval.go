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
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/pkg/ql"
	"slices"
	"strings"
)

type (
	// LogCondEval describes the dialect/grammar of how to interpret
	// the AST tree nodes for `logs condition` expression
	LogCondEval struct {
		dialect map[string]ql.ParamDialect[*solaris.Log, any]
	}
)

// LikeWildcard specifies which wildcard symbol should be used
// in `like` expression of `logs condition`
const LikeWildcard = '%'

// NewLogCondEval creates the new LogCondEval
func NewLogCondEval(dialect map[string]ql.ParamDialect[*solaris.Log, any]) *LogCondEval {
	return &LogCondEval{dialect: dialect}
}

// Eval evaluates `logs condition` for a specific log entity
func (lce LogCondEval) Eval(log *solaris.Log, expr *ql.Expression) (bool, error) {
	for _, oc := range expr.Or {
		orOk := true
		for _, xc := range oc.And {
			var (
				andOk bool
				err   error
			)
			if xc.Expr != nil {
				andOk, err = lce.Eval(log, xc.Expr)
			} else {
				andOk, err = lce.eval(log, xc.Cond)
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

func (lce LogCondEval) eval(log *solaris.Log, c *ql.Condition) (bool, error) {
	// first param
	p1, ok := lce.dialect[c.FirstParam.ID()]
	if !ok {
		return false, fmt.Errorf("unknown parameter %q: %w", c.FirstParam.Name(), errors.ErrInvalid)
	}
	if p1.Flags&ql.PfLValue == 0 {
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
	p2, ok := lce.dialect[c.SecondParam.ID()]
	if !ok {
		return false, fmt.Errorf("unknown second parameter %s: %w",
			c.SecondParam.Name(), errors.ErrInvalid)
	}
	if p2.Flags&ql.PfRValue == 0 {
		return false, fmt.Errorf("parameter %q cannot be on the right side of the condition: %w",
			c.SecondParam.Name(), errors.ErrInvalid)
	}
	// param values
	getParamValues := func() (any, any, error) {
		v1, err := p1.Value(&c.FirstParam, log)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get parameter %q value for the operation %q: %w",
				c.FirstParam.Name(), c.Op, errors.ErrInvalid)
		}
		v2, err := p2.Value(c.SecondParam, log)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get parameter %q value for the operation %q: %w",
				c.SecondParam.Name(), c.Op, errors.ErrInvalid)
		}
		return v1, v2, nil
	}

	// eval
	switch op := strings.ToUpper(c.Op); op {
	case "<", ">", "<=", ">=", "!=", "=":
		if p1.Flags&ql.PfComparable == 0 {
			return false, fmt.Errorf("the first parameter %q is not applicable for the operation %q: %w",
				c.FirstParam.Name(), c.Op, errors.ErrInvalid)
		}
		if p2.Flags&ql.PfComparable == 0 {
			return false, fmt.Errorf("the first parameter %s is not applicable for the operation %q: %w",
				c.SecondParam.Name(), c.Op, errors.ErrInvalid)
		}
		v1, v2, err := getParamValues()
		if err != nil {
			return false, err
		}
		return compare(v1.(string), v2.(string), op), nil
	case "IN":
		if p1.Flags&ql.PfInLike == 0 {
			return false, fmt.Errorf("the first parameter %q is not applicable for the IN : %w",
				c.FirstParam.Name(), errors.ErrInvalid)
		}
		if c.SecondParam.ID() != ql.ArrayParamID {
			return false, fmt.Errorf("the second parameter %q must be an array: %w",
				c.SecondParam.Name(), errors.ErrInvalid)
		}
		v1, v2, err := getParamValues()
		if err != nil {
			return false, err
		}
		return in(v1.(string), v2.([]string)), nil
	case "LIKE":
		if p1.Flags&ql.PfInLike == 0 {
			return false, fmt.Errorf("the first parameter %q is not applicable for the LIKE : %w",
				c.FirstParam.Name(), errors.ErrInvalid)
		}
		if c.SecondParam.ID() != ql.StringParamID {
			return false, fmt.Errorf("the right value(%q) of LIKE must be a string: %w",
				c.SecondParam.Name(), errors.ErrInvalid)
		}
		v1, v2, err := getParamValues()
		if err != nil {
			return false, err
		}
		return like(v1.(string), v2.(string)), nil
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

func like(s, pat string) bool {
	if pat == "" {
		return s == ""
	}
	if len(pat) == 1 && pat[0] == LikeWildcard {
		return true
	}
	match := make([][]bool, len(pat)+1)
	for i := 0; i < len(match); i++ {
		match[i] = make([]bool, len(s)+1)
	}
	match[0][0] = true
	for i := 0; i < len(pat); i++ {
		if pat[i] == LikeWildcard {
			match[i+1][0] = match[i][0]
		}
	}
	for i := 0; i < len(pat); i++ {
		for j := 0; j < len(s); j++ {
			if pat[i] == LikeWildcard {
				match[i+1][j+1] = match[i][j] || match[i][j+1] || match[i+1][j]
			} else if pat[i] == s[j] {
				match[i+1][j+1] = match[i][j]
			}
		}
	}
	return match[len(pat)][len(s)]
}
