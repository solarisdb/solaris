// Copyright 2023 The acquirecloud Authors
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
package config

import (
	"encoding/json"
	"fmt"
	"github.com/solarisdb/solaris/golibs/logging"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/ghodss/yaml"
	"github.com/solarisdb/solaris/golibs/errors"
)

type (
	// Enricher interface provides some helper functions to work with the configuration structures.
	// It keeps a structure value by the type T and allows to load its value from a file,
	// apply other values from other enricher, created for the same type T, and apply environment
	// variables to the structure.
	//
	// The following contract is applied to the type T:
	// - only the exported fields (started from the capital letter) will be updated
	// - the fields, may have JSON annotation, where the JSON field name value can be used as an alias,
	//   for example, FieldA int `json:"abc"` <- the field may be addressed either as "fieldA" or "abc"
	// - all the fields' names are case-insensitive, so values FIELDA, fielda, ABC, abc etc. will match
	//   to the fields in the previous example
	// - the reading from YAML files is defined by the same (JSON name) annotations
	Enricher[T any] interface {
		// LoadFromFile allows to load the structure's fields from the YAML or JSON file.
		// Which format is used, is defined by the file extension (.json or .yaml)
		// The structure fields may be annotated (and maybe not) by the standard json:"..." tag
		// the first field in the tag will be used as an alias for the field, but the field also
		// may be addressed by its name, if the annotation is not specified
		LoadFromFile(fileName string) error

		// LoadFromJSONFile loads the context from the jsonFileName and try to unmarshal it as
		// JSON. If the jsonFileName is empty, nothing happens and the function immediately returns
		// nil. If the file doesn't exist, the function will return errors.NotExist error, otherwise
		// it will parse the contest and return nil if it could unmarshal it as JSON or the
		// unmarshalling JSON error.
		LoadFromJSONFile(jsonFileName string) error

		// LoadFromYAMLFile loads the context from the yamlFileName and try to unmarshal it as
		// YAML. If the yamlFileName is empty, nothing happens and the function immediately returns
		// nil. If the file doesn't exist, the function will return errors.NotExist error, otherwise
		// it will parse the contest and return nil if it could unmarshal it as YAML or the
		// unmarshalling YAML error.
		LoadFromYAMLFile(yamlFileName string) error

		// ApplyOther allows applying the T structure value by another enricher. The deep
		// apply will be done, which means that all fields from the enricher e, which are not
		// equal to its zero values will overwrite the values of the current enricher value.
		// It means e's non-zero values will overwrite the values from the current enricher
		ApplyOther(e Enricher[T]) error

		// ApplyEnvVariables will scan the existing environment variables and try to apply that
		// one, which name starts from prefix. The fields values will be separated by sep. The
		// variable will represent a path to the required value in the target structure.
		//
		// Example:
		// Inner struct {
		// 		Val int
		// 		StrPtr *string `json:"haha"`
		// }
		// T struct {
		// 	  	Field int
		//		InnerS *Inner
		// }
		// ...
		// The Enricher for the type T will allow to address the following fields with the following
		// format (separated by dots for example):
		//	- Field
		//  - InnerS.Val
		//  - Inners.StrPtr or Inners.haha (alias of Inners.StrPtr)
		//
		// To update them the following variables will make sense for the following call ApplyEnvVariables("MyServer", "_"):
		// 	- MYSERVER_FIELD
		//	- MYSERVER_INNERS_VAL
		//	- MYSERVER_INNERS_STRPTR or MYSERVER_INNERS_HAHA
		//
		// the environment variables are case-insensitive, so MYSERVER_FIELD and MyServer_fielD - will make sense.
		//
		// The variables values should be JSON values. For simple types like int or string, it can be normal value like
		// 123, "hello world" etc.
		// For complex types like slices, maps, structs, JSON format can be used. For example. above the following
		// variable makes sense:
		// MYSERVER_INNERS={"val":123, "haha": "hoho"}
		//
		// It maybe the case when a variable MYSERVER_INNERS_VAL will come before MYSERVER_INNERS, this case the
		// value set by the variable MYSERVER_INNERS_VAL will be overwritten. We consider the case corner
		// and the developers should avoid such kind of unambiguity.
		ApplyEnvVariables(prefix, sep string) error
		// ApplyKeyValues allows to apply the key-value pairs to the structure. The key-value pairs assignment rules
		// are the same as for the ApplyEnvVariables function.
		ApplyKeyValues(prefix, sep string, keyValues map[string]string)

		// Value returns the enricher current value
		Value() T
	}

	enricher[T any] struct {
		log logging.Logger
		val T
	}
)

// NewEnricher constructs new Enricher for the type T
func NewEnricher[T any](val T) Enricher[T] {
	tp := reflect.TypeOf(val)
	if tp.Kind() != reflect.Struct {
		panic(fmt.Sprintf("only structs are eaceptable in the Enricher, but got the type %s", tp.Kind()))
	}
	return newEnricher(val)
}

func (e *enricher[T]) LoadFromFile(fileName string) error {
	if fileName == "" {
		e.log.Infof("don't load data from a file name, the file name is not provided")
		return nil
	}

	fn := strings.Trim(strings.ToLower(fileName), " ")
	switch {
	case strings.HasSuffix(fn, ".yaml"):
		return e.LoadFromYAMLFile(fileName)
	case strings.HasSuffix(fileName, ".json"):
		return e.LoadFromJSONFile(fileName)
	default:
		return fmt.Errorf("cannot recognize file format %s, expecting .json or .yaml: %w", fileName, errors.ErrInvalid)
	}
}

func (e *enricher[T]) LoadFromJSONFile(jsonFileName string) error {
	if jsonFileName == "" {
		e.log.Infof("the function LoadFromJSONFile() is called with empty file name value, do nothing")
		return nil
	}

	e.log.Infof("reading JSON data from %s", jsonFileName)
	buf, err := os.ReadFile(jsonFileName)
	if err != nil {
		return fmt.Errorf("could not read file %s: %w", jsonFileName, err)
	}
	err = json.Unmarshal(buf, &e.val)
	if err != nil {
		return fmt.Errorf("could not unmarshal json file(%s): %w", jsonFileName, err)
	}
	e.log.Infof("value is unmarshaled as JSON")
	return nil
}

func (e *enricher[T]) LoadFromYAMLFile(yamlFileName string) error {
	if yamlFileName == "" {
		e.log.Infof("the function LoadFromYAMLFile() is called with empty file name value, do nothing")
		return nil
	}

	e.log.Infof("reading YAML data from %s", yamlFileName)
	buf, err := os.ReadFile(yamlFileName)
	if err != nil {
		return fmt.Errorf("could not read file %s: %w", yamlFileName, err)
	}
	err = yaml.Unmarshal(buf, &e.val)
	if err != nil {
		return fmt.Errorf("could not unmarshal yaml file(%s): %w", yamlFileName, err)
	}
	e.log.Infof("value is unmarshaled as YAML")
	return nil
}

func (e *enricher[T]) ApplyOther(other Enricher[T]) error {
	otherE := other.(*enricher[T])
	otherVal := &otherE.val
	otherT := reflect.TypeOf(otherVal)
	eVal := &e.val
	targetT := reflect.TypeOf(eVal)
	if otherT != targetT {
		return fmt.Errorf("types are incompatible, the current enricher type is %s, the other enricher type is %s", targetT, otherT)
	}
	return e.applyValues(reflect.ValueOf(otherVal), reflect.ValueOf(eVal))
}

func (e *enricher[T]) ApplyEnvVariables(prefix, sep string) error {
	e.log.Infof("apply environment variables with the prefix %s", prefix)
	rawEnv := os.Environ()
	env := make(map[string]string)
	for _, v := range rawEnv {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) != 2 {
			e.log.Warnf("the environment variable %s is not valid, skip it", v)
			continue
		}
		env[strings.ToLower(parts[0])] = parts[1]
	}
	e.ApplyKeyValues(prefix, sep, env)
	return nil
}

func (e *enricher[T]) Value() T {
	return e.val
}

func newEnricher[T any](val T) *enricher[T] {
	e := new(enricher[T])
	e.val = val
	e.log = logging.NewLogger("config.enricher." + reflect.TypeOf(val).Name())
	return e
}

func makeEnvPrefix(prefix, sep string) string {
	if prefix == "" {
		return ""
	}
	return strings.ToUpper(prefix) + strings.ToUpper(sep)
}

func (e *enricher[T]) ApplyKeyValues(prefix, sep string, keyValues map[string]string) {
	sep = strings.ToUpper(sep)
	envPfx := makeEnvPrefix(prefix, sep)
	for key, value := range keyValues {
		key := strings.ToUpper(key)
		if strings.HasPrefix(key, envPfx) {
			ok := e.assignStruct(&e.val, key[len(envPfx):], sep, value)
			if ok {
				e.log.Debugf("updating target value by the key=%s, value=%s", key, value)
			} else {
				e.log.Debugf("the key=%s with value=%s cannot be applied (no mathed fields)", key, value)
			}
			e.log.Infof("applying variable %s: %t", key, ok)
		}
	}
}

func (e *enricher[T]) applyValues(other, target reflect.Value) error {
	if other.IsZero() {
		return nil
	}
	if other.Type().Kind() == reflect.Ptr {
		if target.IsNil() {
			target.Set(reflect.New(target.Type().Elem()))
		}
		return e.applyValues(other.Elem(), target.Elem())
	}
	if other.Type().Kind() != reflect.Struct {
		target.Set(other)
		return nil
	}
	for fi := 0; fi < other.NumField(); fi++ {
		fo := other.Field(fi)
		ft := target.Field(fi)
		err := e.applyValues(fo, ft)
		if err != nil {
			return err
		}
	}
	return nil
}

// assignStruct assigns a field addressed by the path with a seaprator sep to the value v,
// which is a json encoded string. s must be a pointer to a structure. path contains the
// name of fields or aliases (json:"name,..." annotation) separated by sep. If a field is
// not exported or cannot be changed the function will panicing.
//
// Some objects in the path may not exist yet (pointers to values or structs) with nil
// value. This case the function will create the all objects needed in the path, but only in
// the case if the path is found and the value is assigned.
//
// It returns true if the value was found and set or false, if the path was not found.
func (e *enricher[T]) assignStruct(s any, path, sep string, v string) bool {
	tp := reflect.TypeOf(s)
	if !isStructPtr(tp) {
		e.log.Warnf("could not assign value for the non-structure type")
		return false
	}
	val := reflect.ValueOf(s)

	fieldName := path
	if idx := strings.Index(path, sep); idx > -1 {
		fieldName = path[:idx]
		path = path[idx+len(sep):]
	} else {
		path = ""
	}
	if fieldName == "" {
		e.log.Warnf("could not assign value for the empty field name")
		return false
	}

	for fi := 0; fi < val.Elem().NumField(); fi++ {
		f := val.Elem().Field(fi)
		fTags := string(tp.Elem().Field(fi).Tag)
		fName := strings.ToUpper(tp.Elem().Field(fi).Name)
		alias := getAlias(fTags)
		if fieldName == fName || fieldName == alias {
			if path != "" {
				obj := f.Interface()
				oVal := reflect.ValueOf(obj)
				oType := reflect.TypeOf(obj)
				res := false
				if oType.Kind() != reflect.Ptr {
					objptr := reflect.New(oType)
					objptr.Elem().Set(oVal)
					res = e.assignStruct(objptr.Interface(), path, sep, v)
					obj = objptr.Elem().Interface()
					oVal = reflect.ValueOf(obj)
				} else {
					if oVal.IsNil() {
						obj = reflect.New(oType.Elem()).Interface()
					}
					res = e.assignStruct(obj, path, sep, v)
					oVal = reflect.ValueOf(obj)
				}
				if res {
					f.Set(oVal)
				}
				return res
			}
			if !f.CanSet() {
				panic(fmt.Sprintf("could not set value to the field %s, it is not settable", fName))
			}
			err := setFieldValueByString(f, v)
			if err != nil {
				panic(fmt.Sprintf("could not set value %s to the field %s: %s", v, fName, err))
			}
			return true
		}
	}
	return false
}

func isStructPtr(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct
}

// setFieldValueByString receives a field value and a string which should be assigned to
// it. Numerical and string values are supported as is, all other types should be provided
// in the json formats.
// Examples:
// int: 1234
// string: la la la
// []string: ["aaa", "bbbb"]
// map[int]string: {1: "sss"}
// etc.
func setFieldValueByString(field reflect.Value, s string) error {
	if len(s) == 0 {
		return nil
	}

	obj := reflect.New(field.Type()).Interface()
	if ifStringUnderlying(field.Type()) && !isQuoted(s) {
		s = strconv.Quote(s)
	}

	err := json.Unmarshal([]byte(s), obj)
	if err != nil {
		return err
	}

	field.Set(reflect.ValueOf(obj).Elem())
	return nil
}

func isQuoted(s string) bool {
	s = strings.Trim(s, " ")
	if len(s) < 2 {
		return false
	}
	return s[0] == '"' && s[len(s)-1] == '"'
}

func ifStringUnderlying(tp reflect.Type) bool {
	if tp.Kind() == reflect.Ptr {
		return ifStringUnderlying(tp.Elem())
	}
	return tp.Kind() == reflect.String
}

func getAlias(tags string) string {
	name := "json"
	if len(name) >= len(tags) {
		return ""
	}

	i := strings.Index(tags, name)
	if i < 0 {
		return ""
	}

	tags = tags[i+len(name):]
	tags = strings.TrimLeft(tags, " ")
	if len(tags) == 0 || tags[0] != ':' {
		return ""
	}
	tags = strings.TrimLeft(tags[1:], " ")

	if len(tags) == 0 || tags[0] != '"' {
		return ""
	}
	tags = tags[1:]

	for i = 0; i < len(tags) && tags[i] != '"'; i++ {
		if tags[i] == '\\' {
			i++
		}
	}
	if i >= len(tags) {
		return ""
	}

	tags = tags[:i]
	idx := strings.Index(tags, ",")
	if idx == -1 {
		return strings.ToUpper(tags)
	}
	return strings.ToUpper(tags[:idx])
}
