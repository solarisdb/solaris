package ql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_compare(t *testing.T) {
	assert.True(t, compare("abc", "abc", "="))
	assert.True(t, compare("abc", "abc", "<="))
	assert.True(t, compare("abc", "abc", ">="))
	assert.True(t, compare("ab", "abc", "<"))
	assert.True(t, compare("abcd", "abc", ">"))
	assert.True(t, compare("abc", "def", "!="))
	assert.False(t, compare("ab", "abc", ">"))
	assert.False(t, compare("abc", "abc", "!="))
}

func Test_in(t *testing.T) {
	assert.True(t, in("abc", []string{"abc"}))
}
