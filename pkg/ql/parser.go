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
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"strings"
)

type (
	// Expression is an AST element which describes a series of OR conditions
	Expression struct {
		Or []*OrCondition `@@ { "OR" @@ }`
	}

	// OrCondition is an AST element which describes a series of AND conditions
	OrCondition struct {
		And []*XCondition `@@ { "AND" @@ }`
	}

	// XCondition is an AST element which groups either a Condition object or an Expression object
	XCondition struct {
		Not  bool        ` [@"NOT"] `
		Cond *Condition  `( @@`
		Expr *Expression `| "(" @@ ")")`
	}

	// Condition is a unary or binary logical operation which has first mandatory param and
	// optional operation and second param
	Condition struct {
		FirstParam  Param  `  @@`
		Op          string ` {@("<"|">"|">="|"<="|"!="|"="|"IN"|"LIKE")`
		SecondParam *Param ` @@}`
	}

	// Param describes a parameter either a constant (string or number), function, identifier or an array of constants
	Param struct {
		Const      *Const    ` @@`
		Function   *Function ` | @@`
		Identifier string    ` | @Ident`
		Array      []*Const  `|"[" (@@ {"," @@})?"]"`
	}

	// Const contains the constant either string or float32 value
	Const struct {
		Number *float32 ` @Number`
		String *string  ` | @String`
	}

	// Function is a functional parameter
	Function struct {
		Name   string   ` @Ident `
		Params []*Param ` "(" (@@ {"," @@})? ")"`
	}
)

var (
	sqlLexer = lexer.MustSimple([]lexer.SimpleRule{
		{`Keyword`, `(?i)\b(AND|OR|NOT|IN|LIKE)\b`},
		{`Ident`, `[a-zA-Z_][a-zA-Z0-9_]*`},
		{`Number`, `[-+]?\d*\.?\d+([eE][-+]?\d+)?`},
		{`String`, `'[^']*'|"[^"]*"`},
		{`Operators`, `!=|<=|>=|[,()=<>\]\[]`},
		{"whitespace", `\s+`},
	})

	parser = participle.MustBuild[Expression](
		participle.Lexer(sqlLexer),
		participle.Unquote("String"),
		participle.CaseInsensitive("Keyword"),
	)
)

const (
	StringParamID = "__string__"
	NumberParamID = "__number__"
	ArrayParamID  = "__array__"
)

// ID returns the param id by its type:
// - string: StringParamID
// - number: NumberParamID
// - function: the function name
// - identifier: the identifier name
// - array: ArrayParamID
func (p Param) ID() string {
	if p.Const != nil {
		if p.Const.String != nil {
			return StringParamID
		}
		return NumberParamID
	}
	if p.Function != nil {
		return p.Function.Name
	}
	if p.Identifier != "" {
		return p.Identifier
	}
	return ArrayParamID
}

// Name returns "value" of the constants (strings, numbers and the arrays) and names for the functions and identifiers
func (p Param) Name(full bool) string {
	if p.Const != nil {
		return p.Const.Value()
	}
	if p.Function != nil {
		return p.Function.Name
	}
	if p.Identifier != "" {
		return p.Identifier
	}

	var sb strings.Builder
	sb.WriteString("[")
	for i, c := range p.Array {
		if i > 0 {
			sb.WriteString(", ")
		}
		if !full && i > 3 && len(p.Array) > 10 {
			sb.WriteString(fmt.Sprintf("... and %d more", len(p.Array)-4))
			break
		}
		sb.WriteString(c.Value())
	}
	sb.WriteString("]")
	return sb.String()
}

// Value returns string value of the constant
func (c Const) Value() string {
	if c.String != nil {
		return *c.String
	}
	return fmt.Sprintf("%f", *c.Number)
}

// Parse parses the expr and in case of success returns AST
func Parse(expr string) (*Expression, error) {
	expr = strings.TrimSpace(expr)
	if len(expr) == 0 {
		return &Expression{}, nil
	}
	e, err := parser.ParseString("", expr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse expression=%q: %w", expr, err)
	}
	return e, nil
}
