// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package buildinfo reports what a binary was built from: the version,
// commit and date release builds stamp with -ldflags, and the Go toolchain
// and VCS settings the build recorded.
package buildinfo

import (
	"runtime/debug"
	"strings"
)

// Version is stamped by release builds with -ldflags.
var Version = "dev"

// GitSHA is stamped by release builds with -ldflags.
var GitSHA = ""

// BuildDate is stamped by release builds with -ldflags.
var BuildDate = ""

// Info is a binary's build: its version, Go version and VCS state.
type Info struct {
	Version     string `json:"version"`
	GoVersion   string `json:"go"`
	VCSRevision string `json:"vcs"`
	VCSTime     string `json:"vcs_time,omitempty"`
	VCSModified string `json:"vcs_modified,omitempty"`
	GitSHA      string `json:"git_sha,omitempty"`
	BuildDate   string `json:"build_date,omitempty"`
}

// Current returns this binary's build: the stamped Version (else the main
// module's version), the Go version and VCS settings from the build info,
// and the stamped GitSHA and BuildDate; "unknown" where nothing says.
func Current() Info {
	info := Info{
		Version:     "unknown",
		GoVersion:   "unknown",
		VCSRevision: "unknown",
	}
	if version := strings.TrimSpace(Version); version != "" && version != "dev" {
		info.Version = version
	} else if bi, ok := debug.ReadBuildInfo(); ok {
		if main := strings.TrimSpace(bi.Main.Version); main != "" {
			info.Version = main
		}
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		if bi.GoVersion != "" {
			info.GoVersion = bi.GoVersion
		}
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				if s.Value != "" {
					info.VCSRevision = s.Value
				}
			case "vcs.time":
				if s.Value != "" {
					info.VCSTime = s.Value
				}
			case "vcs.modified":
				if s.Value != "" {
					info.VCSModified = s.Value
				}
			}
		}
	}
	if gitSHA := strings.TrimSpace(GitSHA); gitSHA != "" {
		info.GitSHA = gitSHA
	}
	if buildDate := strings.TrimSpace(BuildDate); buildDate != "" {
		info.BuildDate = buildDate
	}
	return info
}
