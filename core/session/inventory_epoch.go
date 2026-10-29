// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// reannounced is closed when every agent session here should rewrite its
// inventory record now (reannounceAll); a session then takes the next one.
func (d *Dispatcher) reannounced() <-chan struct{} {
	d.reannounceMu.Lock()
	defer d.reannounceMu.Unlock()
	if d.reannounce == nil {
		d.reannounce = make(chan struct{})
	}
	return d.reannounce
}

// reannounceAll wakes every agent session here to rewrite its record now
// (refresh gates lifted).
// ponytail: every session writes at once (one write each); spread them if a
// replica with very many agents makes that burst matter.
func (d *Dispatcher) reannounceAll() {
	d.InventoryRefreshByAgentID.Clear()
	d.reannounceMu.Lock()
	defer d.reannounceMu.Unlock()
	if d.reannounce != nil {
		close(d.reannounce)
	}
	d.reannounce = make(chan struct{})
}
