// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package repocheck

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

// moduleRoot walks up from the working directory to the go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the working directory")
		}
		dir = parent
	}
}

// TestPublicPackagesAreDocumented fails on a public package (not under
// internal/, cmd/, examples/ or testdata/, not main) without a package doc
// comment, and on an exported symbol of one without a doc comment: types,
// functions, methods of exported types, and consts and vars (a grouped
// declaration's doc covers its members).
func TestPublicPackagesAreDocumented(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	pkgs := map[string][]*ast.File{} // dir -> files
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			switch {
			case rel == ".":
			case strings.HasPrefix(d.Name(), ".") || d.Name() == "testdata" || d.Name() == "vendor",
				rel == "internal" || rel == "cmd" || rel == "examples":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		if f.Name.Name != "main" {
			pkgs[filepath.Dir(rel)] = append(pkgs[filepath.Dir(rel)], f)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	missing := func(pos token.Pos, what string) {
		t.Errorf("%s: %s has no doc comment", fset.Position(pos), what)
	}
	for dir, files := range pkgs {
		hasDoc := false
		for _, f := range files {
			hasDoc = hasDoc || f.Doc != nil
		}
		if !hasDoc {
			t.Errorf("%s: package %s has no package doc comment", dir, files[0].Name.Name)
		}
		for _, f := range files {
			for _, decl := range f.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if !d.Name.IsExported() || d.Doc != nil {
						continue
					}
					if d.Recv != nil {
						if recv := receiverType(d.Recv.List[0].Type); recv == "" || !ast.IsExported(recv) {
							continue
						} else {
							missing(d.Pos(), "method "+recv+"."+d.Name.Name)
							continue
						}
					}
					missing(d.Pos(), "func "+d.Name.Name)
				case *ast.GenDecl:
					if d.Tok == token.IMPORT {
						continue
					}
					for _, spec := range d.Specs {
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if s.Name.IsExported() && s.Doc == nil && d.Doc == nil {
								missing(s.Pos(), "type "+s.Name.Name)
							}
						case *ast.ValueSpec:
							for _, n := range s.Names {
								if n.IsExported() && s.Doc == nil && d.Doc == nil && s.Comment == nil {
									missing(n.Pos(), d.Tok.String()+" "+n.Name)
								}
							}
						}
					}
				}
			}
		}
	}
}

// receiverType is the name of a method's receiver type ("" if unnamed).
func receiverType(e ast.Expr) string {
	for {
		switch x := e.(type) {
		case *ast.StarExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		case *ast.IndexListExpr:
			e = x.X
		case *ast.Ident:
			return x.Name
		default:
			return ""
		}
	}
}
