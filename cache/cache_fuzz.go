// +build gofuzz
package cache

import (
	"fmt"
)

func Fuzz(in []byte) int {
	fuzz := 0
	c := &cache{}

	// -- Size option parsing -- //
	err := Size(string(in))(c)
	sb := c.sizeBytes
	switch {
	case sb != 0 && err != nil:
		panic(fmt.Sprintf("size bytes: %d, err: %s, input: %s", sb, err.Error(), in))
	case sb < 0:
		panic(fmt.Sprintf("size bytes: %d, input: %s", sb, in))
		// TODO: Add further constraints (always fits in memory?)
	}

	return fuzz
}
