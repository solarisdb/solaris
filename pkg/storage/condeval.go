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

import "github.com/solarisdb/solaris/golibs/container"

// LikeWildcard specifies which wildcard symbol should be used
// in `like` expression of `logs condition`
const LikeWildcard = '%'

func like1(s, pat string) bool {
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
	v := []bool{true}
	for i := 0; i < len(pat); i++ {
		if pat[i] == LikeWildcard {
			match[i+1][0] = match[i][0]
			v = append(v, match[i+1][0])
		} else {
			v = append(v, false)
		}
	}
	for i := 0; i < len(pat); i++ {
		container.SliceFill(match[1], false)
		match[0][0] = v[i]
		match[1][0] = v[i+1]
		for j := 0; j < len(s); j++ {
			if pat[i] == LikeWildcard {
				match[1][j+1] = match[0][j] || match[0][j+1] || match[1][j]
			} else if pat[i] == s[j] {
				match[1][j+1] = match[0][j]
			}
		}
		copy(match[0], match[1])
		//copy(match[1], match[2])
	}
	return match[1][len(s)]
}

func like(s, pat string) bool {
	if pat == "" {
		return s == ""
	}
	if len(pat) == 1 && pat[0] == LikeWildcard {
		return true
	}

	prev := make([]bool, len(s)+1)
	next := make([]bool, len(s)+1)
	prev[0] = true

	for i := 0; i < len(pat); i++ {
		container.SliceFill(next, false)
		if pat[i] == LikeWildcard {
			next[0] = prev[0]
		}
		for j := 0; j < len(s); j++ {
			if pat[i] == LikeWildcard {
				next[j+1] = prev[j] || prev[j+1] || next[j]
			} else if pat[i] == s[j] {
				next[j+1] = prev[j]
			}
		}
		prev, next = next, prev
	}
	return prev[len(s)]
}
