package config

import (
	"bytes"
	"sort"
)

func newBytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func sortStrings(s []string) { sort.Strings(s) }
