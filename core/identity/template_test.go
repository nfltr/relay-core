// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"errors"
	"strings"
	"testing"
)

func TestTemplate(t *testing.T) {
	fields := map[string]string{"cn": "build-01", "o": "acme", "email": "a@x.io", "bad": "a:b", "slash": "a/b", "long": strings.Repeat("x", MaxMapped)}
	get := func(n string) (string, bool) { v, ok := fields[n]; return v, ok }
	for _, c := range []struct{ rule, want string }{
		{"{cn}", "build-01"},
		{"team-{o}", "team-acme"},
		{"{o}.{cn}", "acme.build-01"},
		{"{email}", "a@x.io"},
	} {
		tm, err := ParseTemplate(c.rule)
		if err != nil {
			t.Fatalf("%s: %v", c.rule, err)
		}
		if got, err := tm.Expand(get); err != nil || got != c.want {
			t.Fatalf("%s = %q, %v; want %q", c.rule, got, err, c.want)
		}
	}
	for _, rule := range []string{"", "plain", "{", "}", "{cn", "cn}", "{}", "{c n}", "a:{cn}", "a/{cn}", "{cn}}", "x {cn}"} {
		if _, err := ParseTemplate(rule); err == nil {
			t.Fatalf("rule %q parsed", rule)
		}
	}
	for _, rule := range []string{"{missing}", "{bad}", "{slash}", "{long}x"} {
		tm, err := ParseTemplate(rule)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tm.Expand(get); !errors.Is(err, ErrUnmapped) {
			t.Fatalf("%s expanded: %v", rule, err)
		}
	}
	if _, err := (Template{}).Expand(get); !errors.Is(err, ErrUnmapped) {
		t.Fatal("the zero template expanded")
	}
}

// FuzzTemplate: whatever the rule and the credential's fields, a mapped
// value is 1..MaxMapped allowed characters (so never a namespace, an
// address separator or a space), and a rule that parses names a field.
func FuzzTemplate(f *testing.F) {
	f.Add("{cn}", "build-01", "acme")
	f.Add("team-{o}.{cn}", "a:b", "x/y")
	f.Add("{sub}", "", "")
	f.Add("{{cn}}", "}", "{")
	f.Fuzz(func(t *testing.T, rule, a, b string) {
		tm, err := ParseTemplate(rule)
		if err != nil {
			return
		}
		if len(tm.Fields()) == 0 {
			t.Fatalf("rule %q parsed with no field", rule)
		}
		i := 0
		out, err := tm.Expand(func(string) (string, bool) {
			i++
			if i%2 == 1 {
				return a, true
			}
			return b, b != "skip"
		})
		if err != nil {
			if !errors.Is(err, ErrUnmapped) {
				t.Fatalf("Expand error %v is not ErrUnmapped", err)
			}
			return
		}
		if out == "" || len(out) > MaxMapped || strings.IndexFunc(out, func(r rune) bool { return !MappedChar(r) }) >= 0 {
			t.Fatalf("rule %q mapped %q", rule, out)
		}
	})
}
