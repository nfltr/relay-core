// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"fmt"
	"testing"

	"github.com/nfltr/relay-core/relaycoretest"
)

// exampleT stands in for a test's *testing.T in this example.
type exampleT struct {
	testing.TB
	cleanups []func()
}

func (*exampleT) Helper()                           {}
func (t *exampleT) Cleanup(f func())                { t.cleanups = append(t.cleanups, f) }
func (*exampleT) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }
func (*exampleT) Errorf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func ExampleNew() {
	t := &exampleT{} // in a test: the test's t
	defer func() {
		for _, f := range t.cleanups {
			f()
		}
	}()

	r := relaycoretest.New(t)
	// Agents connect to r.GRPC with r.Key; the key's clients reach them on r.Proxy.
	fmt.Println(r.GRPC != "", r.Proxy != "", len(r.Key) > 0)
	// Output: true true true
}
