// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharestore

import (
	"fmt"
	"strings"
)

// The presets a share is issued with (Policy.Preset).
const (
	PresetPublic  = "public"
	PresetReview  = "review"
	PresetWebhook = "webhook"
)

// AccessLabel names the share's kind for people (the dashboard, the issue
// answer).
func (p Policy) AccessLabel() string {
	switch p.Preset {
	case PresetReview:
		return "Review Share"
	case PresetWebhook:
		return "Webhook Share"
	default:
		if p.Protected() {
			return "Protected"
		}
		return "Public"
	}
}

// AccessSummary lists the share's access controls for people.
func (p Policy) AccessSummary() string {
	parts := make([]string, 0, 4)
	if p.BasicAuth != "" {
		parts = append(parts, "browser-friendly Basic Auth")
	}
	if p.BearerToken != "" {
		parts = append(parts, "Bearer token")
	}
	if n := len(p.RequiredHeaders); n > 0 {
		parts = append(parts, fmt.Sprintf("%d required header%s", n, plural(n)))
	}
	if n := len(p.AllowedIPs); n > 0 {
		parts = append(parts, fmt.Sprintf("%d allowed IP range%s", n, plural(n)))
	}
	if len(parts) > 0 {
		return strings.Join(parts, "; ")
	}
	switch p.Preset {
	case PresetReview:
		return "review share"
	case PresetWebhook:
		return "webhook/API share"
	default:
		return "public link"
	}
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
