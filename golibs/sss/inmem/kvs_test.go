package inmem

import (
	"github.com/solarisdb/solaris/golibs/sss"
	"testing"
)

func TestStorage(t *testing.T) {
	sss.TestSimpleStorage(t, NewStorage())
}
