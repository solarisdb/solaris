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
)

type (
	ParamDialect[T, V any] struct {
		Flags int
		Value func(p *Param, obj T) (any, error)
	}
)

var (
	LogsCondDialect = map[string]ParamDialect[*solaris.Log, any]{
		NumberParamID: { // numbers are rvalues only
			Flags: PfRValue | PfComparable,
			Value: func(p *Param, log *solaris.Log) (any, error) {
				return p.Const.Value(), nil
			},
		},
		StringParamID: { // strings are rvalues only
			Flags: PfRValue | PfComparable,
			Value: func(p *Param, log *solaris.Log) (any, error) {
				return p.Const.Value(), nil
			},
		},
		ArrayParamID: { // arrays are rvalues only
			Flags: PfRValue,
			Value: func(p *Param, log *solaris.Log) (any, error) {
				var strArr []string
				for _, elem := range p.Array {
					strArr = append(strArr, elem.Value())
				}
				return strArr, nil
			},
		},
		"id": {
			Flags: PfLValue | PfComparable | PfInLike,
			Value: func(p *Param, log *solaris.Log) (any, error) {
				return log.ID, nil
			},
		},
		"tag": { // tag function is written the way -> 'tag("abc") in ["1", "2", "3"]' or 'tag("t1") = "aaa"'
			Flags: PfLValue | PfComparable | PfRValue | PfInLike,
			Value: func(p *Param, log *solaris.Log) (any, error) {
				if p.Function == nil {
					return "", fmt.Errorf("tag must be a function: %w", errors.ErrInvalid)
				}
				if len(p.Function.Params) != 1 {
					return "", fmt.Errorf("tag() function expects only one parameter - the name of the tag: %w", errors.ErrInvalid)
				}
				if p.Function.Params[0].ID() != StringParamID {
					return "", fmt.Errorf("tag() function expects the tag name (string) as the parameter: %w", errors.ErrInvalid)
				}
				if len(log.Tags) == 0 {
					return "", nil
				}
				return log.Tags[p.Function.Params[0].Name()], nil
			},
		},
	}
)
