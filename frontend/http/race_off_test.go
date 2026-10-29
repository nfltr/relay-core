// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !race

package httpingress

// raceAllocs is what the race detector adds to an allocation budget.
const raceAllocs = 0
