// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoTestSeamsInProduction keeps test-only seams out of the production
// relay: they belong in core/session/sessiontest.
func TestNoTestSeamsInProduction(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if f == "test_hooks.go" {
			t.Errorf("%s: test seams belong in core/session/sessiontest", f)
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(fset, f, src, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range af.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() || fn.Doc == nil {
				continue
			}
			doc := strings.ToLower(fn.Doc.Text())
			if strings.Contains(doc, "for tests") || strings.Contains(doc, "relaytest") {
				t.Errorf("%s: %s is documented as a test seam; move it to core/session/sessiontest", f, fn.Name.Name)
			}
		}
	}
}

// testOnlyDirs hold test support compiled into no relay binary: a use there
// is not a production use.
var testOnlyDirs = map[string]bool{"sessiontest": true, "relaytest": true}

// TestNoExportedSeamsOnlyTestsUse catches a seam the doc-comment check above
// cannot: an exported function or method of this package that production
// code never refers to, because it was exported for a test (or a test
// support package) to call. Make it a documented production query or option
// with a production caller, reach the state through the exported fields (as
// sessiontest does), or drop it.
//
// References are matched by name, so a method that only satisfies an
// interface is found by the interface's method of the same name; the ones
// production calls through a generated interface or the standard library
// are listed in viaInterface.
func TestNoExportedSeamsOnlyTestsUse(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	type decl struct{ file, name string }
	var exported []decl
	used := map[string]bool{}
	err = filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			if n := e.Name(); n == "vendor" || n == ".git" || n == "node_modules" || testOnlyDirs[n] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		af, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		inSession := filepath.Dir(rel) == filepath.Join("core", "session")
		declName := map[*ast.Ident]bool{}
		for _, d := range af.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok {
				declName[fn.Name] = true
				if inSession && fn.Name.IsExported() {
					exported = append(exported, decl{rel, fn.Name.Name})
				}
			}
		}
		ast.Inspect(af, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && !declName[id] {
				used[id.Name] = true
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range exported {
		if !used[d.name] && !viaInterface[d.name] && !usedOutside[d.name] {
			t.Errorf("%s: exported %s has no production caller; it exists for tests", d.file, d.name)
		}
	}
}

// usedOutside are exported functions and methods this module's own code
// never calls that a relay built on it does (the hosted relay, through the
// public API): queries of its agents and the gRPC tunnel its replica link
// serves. A new name here is public API; record it in CHANGELOG.md.
var usedOutside = map[string]bool{
	"Hello": true, "Verify": true, "AgentsHere": true, "UpdateAgentLabels": true,
	"GetAllAgentInfo": true, "GetAllUpstreamAgentInfo": true, "AgentCount": true,
	"RelayTunnel": true, "ServeEndpoint": true, "WorkerPresent": true,
	"FirstMD": true, "NewInternalAgentLoopback": true, "DispatcherSide": true, "AgentSide": true, "RegisterInternalAgent": true, "NewPodRelayStream": true, "ServeLinkTCP": true, "BindBus": true, "NewReserved": true, "NewSessionListener": true,
}

// viaInterface are the exported methods production reaches only through an
// interface of another package (a generated gRPC service, a standard library
// interface), which a name scan of this repository does not see.
var viaInterface = map[string]bool{
	"AgentTunnel": true, "ConnectHTTP": true, "ConnectTCP": true, // pb.DispatcherServer
	"ConnDown": true,                  // link.Observer
	"Hop":      true,                  // link.ChainObserver
	"Refuse":   true, "PathLen": true, // link.Session (in-band chain sessions)
	"WriteOwned": true, // github.com/nfltr/link's owned-write interface
	"Network":    true, // net.Addr
	"GRPCStatus": true, // status.FromError's interface
}
