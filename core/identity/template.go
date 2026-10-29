// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"errors"
	"fmt"
	"strings"
)

// Template is a provider's rule that makes an owner or a subject from a
// credential's fields (a certificate's subject and SANs, a token's claims):
// text with "{field}" placeholders, e.g. "{o}", "team-{ou}" or "{sub}".
// Fields are the provider's; the result is checked here, the same for
// every provider.
type Template struct{ parts []string } // literal, field, literal, field, ...

// MaxMapped bounds a mapped owner or subject.
const MaxMapped = 128

// ParseTemplate parses rule: "{" and "}" only around a field name of
// letters, digits, '_' and '-', at least one field, literals of mapped
// characters only (MappedChar).
func ParseTemplate(rule string) (Template, error) {
	var t Template
	rest := rule
	for {
		open := strings.IndexByte(rest, '{')
		lit := rest
		if open >= 0 {
			lit = rest[:open]
		}
		if strings.ContainsRune(lit, '}') {
			return Template{}, fmt.Errorf("identity: rule %q: a '}' without its '{'", rule)
		}
		for _, r := range lit {
			if !MappedChar(r) {
				return Template{}, fmt.Errorf("identity: rule %q: %q is not allowed in an owner or subject", rule, r)
			}
		}
		t.parts = append(t.parts, lit)
		if open < 0 {
			break
		}
		end := strings.IndexByte(rest[open:], '}')
		if end < 0 {
			return Template{}, fmt.Errorf("identity: rule %q: a '{' without its '}'", rule)
		}
		field := rest[open+1 : open+end]
		if field == "" || strings.IndexFunc(field, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
		}) >= 0 {
			return Template{}, fmt.Errorf("identity: rule %q: bad field name {%s}", rule, field)
		}
		t.parts = append(t.parts, field)
		rest = rest[open+end+1:]
	}
	if len(t.parts) < 2 {
		return Template{}, fmt.Errorf("identity: rule %q names no field", rule)
	}
	return t, nil
}

// Fields lists the fields t reads.
func (t Template) Fields() []string {
	var out []string
	for i := 1; i < len(t.parts); i += 2 {
		out = append(out, t.parts[i])
	}
	return out
}

// ErrUnmapped is the error of a credential whose fields do not make a
// valid owner or subject under a rule (a field missing or empty, a
// character not allowed, too long).
var ErrUnmapped = errors.New("credential does not map to an owner or subject")

// Expand makes the value: each field from field (ok false: the credential
// has none). The result must be 1..MaxMapped mapped characters.
func (t Template) Expand(field func(name string) (string, bool)) (string, error) {
	if len(t.parts) == 0 {
		return "", ErrUnmapped
	}
	var b strings.Builder
	for i, p := range t.parts {
		if i%2 == 0 {
			b.WriteString(p)
			continue
		}
		v, ok := field(p)
		if !ok || v == "" {
			return "", fmt.Errorf("%w: no %s", ErrUnmapped, p)
		}
		b.WriteString(v)
	}
	out := b.String()
	if len(out) > MaxMapped || strings.IndexFunc(out, func(r rune) bool { return !MappedChar(r) }) >= 0 {
		return "", fmt.Errorf("%w: a value of %d bytes, or a character not allowed", ErrUnmapped, len(out))
	}
	return out, nil
}

// MappedChar reports whether r may appear in a mapped owner or subject:
// ASCII letters, digits and "._@+-". No ':' (the core's namespace
// separator), '/' (owner/name addresses) or space.
func MappedChar(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._@+-", r)
}
