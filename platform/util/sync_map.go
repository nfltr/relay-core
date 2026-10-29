// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util

import "sync"

// SyncMap is a sync.Map typed by key and value. The zero value is empty and
// ready to use.
type SyncMap[K comparable, T any] struct {
	m sync.Map
}

// Load returns key's value and whether it has one.
func (m *SyncMap[K, T]) Load(key K) (T, bool) {
	return Cast[T](m.m.Load(key))
}

// Store sets key's value.
func (m *SyncMap[K, T]) Store(key K, value T) {
	m.m.Store(key, value)
}

// Delete removes key.
func (m *SyncMap[K, T]) Delete(key K) {
	m.m.Delete(key)
}

// CompareAndDelete deletes key when it holds value.
func (m *SyncMap[K, T]) CompareAndDelete(key K, value T) bool {
	return m.m.CompareAndDelete(key, value)
}

// Range calls f for each key and value until f returns false.
func (m *SyncMap[K, T]) Range(f func(key K, value T) bool) {
	m.m.Range(func(key, value any) bool {
		return f(key.(K), value.(T))
	})
}
