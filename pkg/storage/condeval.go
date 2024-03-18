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
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/pkg/ql"
)

// LikeWildcard specifies which wildcard symbol should be used
// in `like` expression of `logs condition`
const LikeWildcard = '%'

var LogsCondEval = &ql.Evaluator[*solaris.Log]{
	Dialect: ql.LogsCondDialect,
	Like: func(s, pat string) bool {
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
	},
}
