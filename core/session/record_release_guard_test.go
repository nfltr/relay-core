// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// TestNoPodOnlyRecordDelete: every path that ends an agent's session here
// (the stream's end, a heartbeat timeout or send failure, the stale
// reaper, Subscribe's end) removes the agent's record through
// releaseAgentRecord, which deletes it only while it names that session:
// a delete by pod alone (DeleteIPs) removed a newer session's record on
// the same replica (relay-server spec/README.md, Replicas finding 1;
// mutant delete-pod-only). DeleteIPs is called only as
// releaseAgentRecord's fallback (an inventory without sessions).
func TestNoPodOnlyRecordDelete(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "releaseAgentRecord" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "DeleteIPs" {
					t.Errorf("%s: %s deletes the agent's record by pod (DeleteIPs); use releaseAgentRecord with the session", fset.Position(sel.Pos()), fn.Name.Name)
				}
				return true
			})
		}
	}
}
