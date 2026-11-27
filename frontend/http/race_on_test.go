// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build race

package httpingress

// raceAllocs is what the race detector adds to an allocation budget: 1 on
// darwin/arm64, 2 on linux/arm64 (the Linux CI runner).
const raceAllocs = 2
