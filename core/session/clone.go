// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "maps"

// cloneStringSlice copies values; empty is nil (an agent's advertised
// capabilities and routes as AgentInfo carries them).
func cloneStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

// cloneStringMap copies values; empty is nil.
func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	maps.Copy(cloned, values)
	return cloned
}
