// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package repocheck

import (
	"fmt"
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestDocsNameNoGhosts: the doc comments of the module's public packages
// (as TestPublicPackagesAreDocumented walks them) name only what exists: no
// unexported identifier, no member a type lacks, no name api/*.txt records
// as removed that the package does not export again (link's and relay's
// test of the same name; lane RCAPI). A doc may qualify a name by its
// package's name ("session.Core"); frontend and core/frontend share theirs,
// so a "frontend." name must be an export of either.
func TestDocsNameNoGhosts(t *testing.T) {
	root := moduleRoot(t)
	pkgs := map[string]docPkg{}   // by package name (frontend: both packages merged)
	dirs := map[string][]string{} // package name -> dirs
	byDir := map[string]string{}  // dir -> package name
	own := map[string]docPkg{}    // by dir
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		switch {
		case rel == ".":
			return nil
		case strings.HasPrefix(d.Name(), ".") || d.Name() == "testdata" || d.Name() == "vendor",
			rel == "internal" || rel == "cmd" || rel == "examples":
			return filepath.SkipDir
		}
		name, ok := pkgName(path)
		if !ok {
			return nil
		}
		p := loadDocPkg(path)
		own[rel], byDir[rel] = p, name
		dirs[name] = append(dirs[name], rel)
		pkgs[name] = merge(pkgs[name], p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	removed := removedNames(t, root, byDir)
	var bad []string
	for dir, p := range own {
		name := byDir[dir]
		scope := pkgs
		if len(dirs[name]) > 1 { // its own names, not its namesake's
			scope = map[string]docPkg{}
			for k, v := range pkgs {
				scope[k] = v
			}
			scope[name] = p
		}
		for where, text := range p.text {
			seen := map[string]bool{}
			for _, tok := range docToken.FindAllString(text, -1) {
				if seen[tok] {
					continue
				}
				seen[tok] = true
				if why := ghost(name, tok, scope, removed); why != "" {
					bad = append(bad, fmt.Sprintf("%s %s's doc: %q %s", dir, where, tok, why))
				}
			}
		}
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Error(b)
	}
}

// merge joins two packages' names (core/frontend and frontend share a
// package name, so "frontend.X" may name either's X).
func merge(a, b docPkg) docPkg {
	if a.names == nil {
		return b
	}
	out := docPkg{text: a.text, names: map[string]bool{}, args: map[string]bool{}, types: map[string][]string{}}
	for _, p := range []docPkg{a, b} {
		for k := range p.names {
			out.names[k] = true
		}
		for k := range p.args {
			out.args[k] = true
		}
		for k, v := range p.types {
			out.types[k] = append(out.types[k], v...)
		}
	}
	return out
}

// pkgName is the package of dir's non-test Go files; false for none or main.
func pkgName(dir string) (string, bool) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.PackageClauseOnly)
		if err != nil {
			panic(err)
		}
		return af.Name.Name, af.Name.Name != "main"
	}
	return "", false
}

// docPkg is what a package's docs say and the names it exports.
type docPkg struct {
	text  map[string]string   // doc comment by what it documents: the package, its exported names, struct fields
	names map[string]bool     // exported top-level names and every exported member name
	args  map[string]bool     // parameter names of exported functions and methods: a doc may name them
	types map[string][]string // exported type -> its exported members
}

func loadDocPkg(dir string) docPkg {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, parser.ParseComments)
	if err != nil {
		panic(err)
	}
	p := docPkg{text: map[string]string{}, names: map[string]bool{}, args: map[string]bool{}, types: map[string][]string{}}
	params := func(fd *ast.FuncDecl) {
		for _, f := range fd.Type.Params.List {
			for _, n := range f.Names {
				p.args[n.Name] = true
			}
		}
	}
	for _, ap := range pkgs {
		d := doc.New(ap, dir, 0)
		p.text["package"] += d.Doc
		values := func(vs []*doc.Value) {
			for _, v := range vs {
				p.text[v.Names[0]] += v.Doc
				for _, n := range v.Names {
					p.names[n] = true
				}
				for _, s := range v.Decl.Specs {
					if vs := s.(*ast.ValueSpec); vs.Names[0].IsExported() {
						p.text[vs.Names[0].Name] += vs.Doc.Text() + vs.Comment.Text()
					}
				}
			}
		}
		values(d.Consts)
		values(d.Vars)
		for _, f := range d.Funcs {
			p.names[f.Name] = true
			p.text[f.Name] += f.Doc
			params(f.Decl)
		}
		for _, ty := range d.Types {
			p.names[ty.Name] = true
			p.text[ty.Name] += ty.Doc
			values(ty.Consts)
			values(ty.Vars)
			member := func(n string) {
				p.names[n] = true
				p.types[ty.Name] = append(p.types[ty.Name], n)
			}
			for _, f := range ty.Funcs {
				p.names[f.Name] = true
				p.text[f.Name] += f.Doc
				params(f.Decl)
			}
			for _, m := range ty.Methods {
				member(m.Name)
				p.text[ty.Name+"."+m.Name] += m.Doc
				params(m.Decl)
			}
			var fields *ast.FieldList
			switch tt := ty.Decl.Specs[0].(*ast.TypeSpec).Type.(type) {
			case *ast.StructType:
				fields = tt.Fields
			case *ast.InterfaceType:
				fields = tt.Methods
			}
			if fields == nil {
				continue
			}
			for _, f := range fields.List {
				for _, n := range f.Names {
					p.text[ty.Name+"."+n.Name] += f.Doc.Text() + f.Comment.Text()
				}
				if ft, ok := f.Type.(*ast.FuncType); ok { // a func field or an interface method
					for _, a := range ft.Params.List {
						for _, n := range a.Names {
							p.args[n.Name] = true
						}
					}
				}
				for _, n := range f.Names {
					if n.IsExported() {
						member(n.Name)
					}
				}
				if len(f.Names) == 0 { // embedded
					if id, ok := ast.Unparen(f.Type).(*ast.Ident); ok && id.IsExported() {
						member(id.Name)
					}
				}
			}
		}
	}
	return p
}

// removedNames are the names api/*.txt records as removed, by package
// name (apidiff's "- ./dir.T.M: removed", "- ./dir.(*T).M: removed").
func removedNames(t *testing.T, root string, byDir map[string]string) map[string]map[string]bool {
	files, err := filepath.Glob(filepath.Join(root, "api", "*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]bool{}
	for _, f := range files {
		data, err := os.ReadFile(f) // #nosec G304 -- api/*.txt of this repository, from Glob
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range strings.Split(string(data), "\n") {
			l, ok := strings.CutSuffix(l, ": removed")
			if !ok || !strings.HasPrefix(l, "- ./") {
				continue
			}
			rest := strings.TrimPrefix(l, "- ./")
			i := strings.LastIndex(rest, "/") + 1
			j := strings.Index(rest[i:], ".") + i
			if j < i {
				continue // a package
			}
			pkg := byDir[rest[:j]]
			l = strings.NewReplacer("(*", "", "(", "", ")", "").Replace(rest[j+1:])
			parts := strings.Split(l, ".")
			if out[pkg] == nil {
				out[pkg] = map[string]bool{}
			}
			out[pkg][parts[len(parts)-1]] = true
		}
	}
	return out
}

var (
	docToken = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*`)
	camelLow = regexp.MustCompile(`^[a-z][a-z0-9]*[A-Z][A-Za-z0-9]*$`)
	// camelWord are words of the docs that look like unexported names:
	// prose, an X.509 field (rfc822Name) and an example's variable
	// (machineTLS).
	camelWord = map[string]bool{"macOS": true, "mTLS": true, "gRPC": true, "iOS": true, "rfc822Name": true, "machineTLS": true}
)

// ghost says why tok, a word of package pkg's docs, names nothing; ""
// when it does or is not a Go name of these packages.
func ghost(pkg, tok string, pkgs map[string]docPkg, removed map[string]map[string]bool) string {
	if strings.HasSuffix(tok, ".tla") || strings.HasSuffix(tok, ".cfg") || strings.HasSuffix(tok, ".pv") {
		return "" // a spec file
	}
	parts := strings.Split(tok, ".")
	if _, ok := pkgs[parts[0]]; ok && len(parts) == 2 && !ast.IsExported(parts[1]) {
		return "" // not a Go name: a protocol id ("p2p.v1")
	}
	if q, ok := pkgs[parts[0]]; ok && len(parts) > 1 {
		pkg, parts = parts[0], parts[1:]
		if !q.names[parts[0]] {
			return "is not an exported name of " + pkg
		}
	}
	p := pkgs[pkg]
	if len(parts) > 1 {
		if members, ok := p.types[parts[0]]; ok {
			for _, m := range members {
				if m == parts[1] {
					return ""
				}
			}
			return "is no exported member of " + parts[0]
		}
		return ""
	}
	w := parts[0]
	if camelLow.MatchString(w) && !camelWord[w] && !p.args[w] {
		return "is unexported: name what the caller can use"
	}
	if removed[pkg][w] && !p.names[w] {
		return "was removed (api/*.txt)"
	}
	return ""
}
