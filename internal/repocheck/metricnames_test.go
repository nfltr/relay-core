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

// TestMetricNamesHaveOnePrefix: every metric the relay defines is named
// nfltr_relay_*, one prefix for v1 (docs/v1-api-review.md "Metric names").
// It reads each prometheus *Opts literal (Namespace, Subsystem, Name) and
// each prometheus.NewDesc name in the module's non-test code; a name part
// that is neither a string literal nor a string constant of its package
// fails too, so no metric escapes the check.
func TestMetricNamesHaveOnePrefix(t *testing.T) {
	const prefix = "nfltr_relay_"
	pkgs := map[string][]*ast.File{}
	fset := token.NewFileSet()
	err := filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || d.Name() == "vendor" || strings.HasPrefix(d.Name(), ".")) && path != "../.." {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		pkgs[filepath.Dir(path)] = append(pkgs[filepath.Dir(path)], f)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, files := range pkgs {
		consts := map[string]string{}
		for _, f := range files {
			ast.Inspect(f, func(x ast.Node) bool {
				if v, ok := x.(*ast.ValueSpec); ok {
					for i, name := range v.Names {
						if i < len(v.Values) {
							if s, ok := literal(v.Values[i]); ok {
								consts[name.Name] = s
							}
						}
					}
				}
				return true
			})
		}
		var value func(e ast.Expr) (string, bool)
		value = func(e ast.Expr) (string, bool) {
			switch e := e.(type) {
			case *ast.Ident:
				s, ok := consts[e.Name]
				return s, ok
			case *ast.BinaryExpr: // a + b
				a, okA := value(e.X)
				b, okB := value(e.Y)
				return a + b, okA && okB && e.Op == token.ADD
			case *ast.CallExpr: // prometheus.BuildFQName(namespace, subsystem, name)
				var parts []string
				if sel, ok := e.Fun.(*ast.SelectorExpr); !ok || !isIdent(sel.X, "prometheus") || sel.Sel.Name != "BuildFQName" {
					return "", false
				}
				for _, a := range e.Args {
					s, ok := value(a)
					if !ok {
						return "", false
					}
					if s != "" {
						parts = append(parts, s)
					}
				}
				return strings.Join(parts, "_"), true
			}
			return literal(e)
		}
		check := func(pos token.Pos, parts ...ast.Expr) {
			n++
			var names []string
			for _, p := range parts {
				if p == nil {
					continue
				}
				s, ok := value(p)
				if !ok {
					t.Errorf("%s: a metric name part the check cannot read (use a string literal or constant)", fset.Position(pos))
					return
				}
				if s != "" {
					names = append(names, s)
				}
			}
			if name := strings.Join(names, "_"); !strings.HasPrefix(name, prefix) {
				t.Errorf("%s: metric %q: every relay metric is named %s*", fset.Position(pos), name, prefix)
			}
		}
		for _, f := range files {
			ast.Inspect(f, func(x ast.Node) bool {
				switch x := x.(type) {
				case *ast.CompositeLit:
					if sel, ok := x.Type.(*ast.SelectorExpr); ok && isIdent(sel.X, "prometheus") && strings.HasSuffix(sel.Sel.Name, "Opts") {
						fields := map[string]ast.Expr{}
						for _, el := range x.Elts {
							if kv, ok := el.(*ast.KeyValueExpr); ok {
								if k, ok := kv.Key.(*ast.Ident); ok {
									fields[k.Name] = kv.Value
								}
							}
						}
						check(x.Pos(), fields["Namespace"], fields["Subsystem"], fields["Name"])
					}
				case *ast.CallExpr:
					if sel, ok := x.Fun.(*ast.SelectorExpr); ok && isIdent(sel.X, "prometheus") && sel.Sel.Name == "NewDesc" && len(x.Args) > 0 {
						check(x.Pos(), x.Args[0])
					}
				}
				return true
			})
		}
	}
	if n < 80 {
		t.Fatalf("found %d metric definitions, want the relay's 89 or so: the scan missed them", n)
	}
}

func literal(e ast.Expr) (string, bool) {
	b, ok := e.(*ast.BasicLit)
	if !ok || b.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(b.Value)
	return s, err == nil
}

func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}
