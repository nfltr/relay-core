// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package repocheck

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestNoProbedRefusedPort fails on a Go test that makes a "refusing" target
// by listening on port 0 and closing the listener: any process may take the
// freed port at once, and the dial meant to be refused reaches a stranger.
// Hold the port instead: dial a listener of the test's own and use the
// connection's local port while the connection lasts (relay-server's
// tcprig.RefusedAddr). The same guard is in nfltr and relay-server.
func TestNoProbedRefusedPort(t *testing.T) {
	var hits []string
	err := filepath.WalkDir("../..", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "vendor" || n == "node_modules" || n == "testdata" || (n != ".." && strings.HasPrefix(n, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return err
		}
		for _, pos := range probedRefusedPorts(f) {
			hits = append(hits, fset.Position(pos).String())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) > 0 {
		t.Fatalf("a listener on port 0 used only for its address and closed (a refused port another process can take; hold it as a held connection's local port):\n%s", strings.Join(hits, "\n"))
	}
}

func TestProbedRefusedPortDetector(t *testing.T) {
	for src, want := range map[string]int{
		`ln, _ := net.Listen("tcp", "127.0.0.1:0"); addr := ln.Addr().String(); _ = ln.Close(); dial(addr)`:           1,
		`ln, err := net.Listen("tcp", ":0"); port := ln.Addr().(*net.TCPAddr).Port; ln.Close(); use(port, err)`:       1,
		`ln, _ := net.Listen("tcp", "127.0.0.1:0"); go serve(ln); addr := ln.Addr().String(); ln.Close(); dial(addr)`: 0, // served
		`ln, _ := net.Listen("tcp", "127.0.0.1:0"); defer ln.Close(); dial(ln.Addr().String())`:                       0, // held
		`ln, _ := net.Listen("tcp", "127.0.0.1:0"); t.Cleanup(func() { ln.Close() }); dial(ln.Addr().String())`:       0, // held
		`if ln, err := net.Listen("tcp", addr); err == nil { _ = ln.Close(); t.Fatal("free") }`:                       0, // a check that addr is held
	} {
		f, err := parser.ParseFile(token.NewFileSet(), "x_test.go", "package x\nfunc f() {"+src+"}", 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(probedRefusedPorts(f)); got != want {
			t.Errorf("%s: %d hits, want %d", src, got, want)
		}
	}
}

// probedRefusedPorts returns each `x, ... := net.Listen(_, "...:0")` whose x
// its function uses only as x.Addr() and an undeferred x.Close().
func probedRefusedPorts(f *ast.File) []token.Pos {
	var out []token.Pos
	ast.Inspect(f, func(n ast.Node) bool {
		var body *ast.BlockStmt
		switch fn := n.(type) {
		case *ast.FuncDecl:
			body = fn.Body
		case *ast.FuncLit:
			body = fn.Body
		}
		if body == nil {
			return true
		}
		ast.Inspect(body, func(m ast.Node) bool {
			if _, ok := m.(*ast.FuncLit); ok {
				return false // its own function
			}
			as, ok := m.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 || !isListenOnPort0(as.Rhs[0]) {
				return true
			}
			if id, ok := as.Lhs[0].(*ast.Ident); ok && id.Name != "_" && onlyAddrAndClose(body, id.Name) {
				out = append(out, as.Pos())
			}
			return true
		})
		return true
	})
	return out
}

func isListenOnPort0(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok || len(call.Args) != 2 {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Listen" {
		return false
	}
	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "net" {
		return false
	}
	lit, ok := call.Args[1].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return false
	}
	s, err := strconv.Unquote(lit.Value)
	return err == nil && strings.HasSuffix(s, ":0")
}

// onlyAddrAndClose: name is closed, outside any defer or function literal,
// and otherwise only asked its address.
func onlyAddrAndClose(body *ast.BlockStmt, name string) bool {
	closed, other := false, false
	var walk func(n ast.Node, nested bool)
	walk = func(n ast.Node, nested bool) {
		ast.Inspect(n, func(m ast.Node) bool {
			switch x := m.(type) {
			case *ast.DeferStmt, *ast.FuncLit:
				if m != n {
					walk(m, true)
					return false
				}
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok && id.Name == name && x.Tok == token.DEFINE {
						for _, r := range x.Rhs {
							walk(r, nested)
						}
						return false
					}
				}
			case *ast.SelectorExpr:
				if id, ok := x.X.(*ast.Ident); ok && id.Name == name {
					switch {
					case x.Sel.Name == "Addr":
					case x.Sel.Name == "Close" && !nested:
						closed = true
					default:
						other = true
					}
					return false
				}
			case *ast.Ident:
				if x.Name == name {
					other = true
				}
			}
			return true
		})
	}
	walk(body, false)
	return closed && !other
}
