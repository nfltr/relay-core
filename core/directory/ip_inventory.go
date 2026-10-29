// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"time"

	"log/slog"

	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/spf13/viper"
)

// ErrAgentNotConnected is a lookup of an agent no replica holds.
var ErrAgentNotConnected = errors.New("agent not connected to any pod")

// ErrUnavailable is a directory read that failed (the shared store is
// down, partitioned or failing fast): whether the agent is connected
// anywhere is unknown. The relay answers it as its own failure
// (ERROR_REASON_DIRECTORY_UNAVAILABLE), never as the agent being offline.
var ErrUnavailable = errors.New("the relay's directory is unavailable")

// IPInventory maps agent ids to the replica address holding each agent's
// session, with the agent's labels and capabilities, each record expiring
// unless renewed.
type IPInventory interface {
	GetIP(ctx context.Context, agentID string) (string, error)
	ListAgentIDs(ctx context.Context) ([]string, error)
	ListAgentRecords(ctx context.Context) ([]AgentRecord, error)
	SetIP(ctx context.Context, agentID string, expiration time.Duration) error
	SetIPWithLabels(ctx context.Context, agentID string, labels map[string]string, expiration time.Duration) error
	SetIPWithAgentInfo(ctx context.Context, agentID string, labels map[string]string, capabilities []string, expiration time.Duration) error
	DeleteIPs(ctx context.Context, agentIDs ...string) error
}

// AgentRecord is one agent as the inventory lists it.
type AgentRecord struct {
	AgentID      string
	PodIP        string
	Labels       map[string]string
	Capabilities []string
}

type noOpInventory struct{}

// NewNoOpInventory returns an inventory that records nothing and finds
// nothing.
func NewNoOpInventory() IPInventory {
	return &noOpInventory{}
}

func (n *noOpInventory) GetIP(ctx context.Context, agentID string) (string, error) {
	return "", nil
}

func (n *noOpInventory) ListAgentIDs(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (n *noOpInventory) ListAgentRecords(ctx context.Context) ([]AgentRecord, error) {
	return nil, nil
}

func (n *noOpInventory) SetIP(ctx context.Context, agentID string, expiration time.Duration) error {
	return nil
}

func (n *noOpInventory) SetIPWithLabels(ctx context.Context, agentID string, labels map[string]string, expiration time.Duration) error {
	return nil
}

func (n *noOpInventory) SetIPWithAgentInfo(ctx context.Context, agentID string, labels map[string]string, capabilities []string, expiration time.Duration) error {
	return nil
}

func (n *noOpInventory) DeleteIPs(ctx context.Context, agentIDs ...string) error {
	return nil
}

type ipInventory struct {
	inventory   Cache
	opTimeout   time.Duration
	loopback    string
	getCacheKey func(agentID string) string
	logger      *slog.Logger
}

// NewIPInventory returns an inventory kept in cache, each operation bounded
// by opTimeout, records keyed by cacheKeyFn (GetDispatcherCacheKey,
// GetStreamerCacheKey); a record naming this replica's own pod IP
// (GetPodIP) reads back as loopbackAddr, for local dispatch.
func NewIPInventory(logger *slog.Logger, cache Cache, opTimeout time.Duration, loopbackAddr string, cacheKeyFn func(string) string) (IPInventory, error) {
	if cache == nil {
		return nil, fmt.Errorf("cache is nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	return &ipInventory{
		inventory:   cache,
		opTimeout:   opTimeout,
		loopback:    loopbackAddr,
		getCacheKey: cacheKeyFn,
		logger:      logger,
	}, nil
}

func (d *ipInventory) activeAgentSetKey() string {
	return "inventory:active:" + d.getCacheKey("")
}

func (d *ipInventory) GetIP(ctx context.Context, agentID string) (podIP string, err error) {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()

	cacheKey := d.getCacheKey(agentID)
	logger := logging.With(d.logger, "agent_id", agentID, "cache_key", cacheKey)

	logger.Debug("getting IP for agentID", "agent_id", agentID, "cache_key", cacheKey)
	value, err := d.inventory.Get(ctx, cacheKey)

	if err != nil {
		if errors.Is(err, ErrNotFound) {
			logger.Debug("agentID not found in cache", "agent_id", agentID)
			return "", fmt.Errorf("%w: %s", ErrAgentNotConnected, agentID)
		}

		logger.Error("error getting IP for agentID", "agent_id", agentID, "cache_key", cacheKey, "error", err)
		return "", fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	// Handle nil pointer (no value found)
	if value == nil {
		logger.Debug("agentID not found in cache", "agent_id", agentID)
		return "", fmt.Errorf("%w: %s", ErrAgentNotConnected, agentID)
	}

	logger.Debug("successfully retrieved IP for agentID", "pod_ip", value.PodIP, "agent_id", agentID)

	// Empty PodIP possible?
	if value.PodIP == "" {
		logger.Warn("empty PodIP retrieved for agentID", "agent_id", agentID)
		return "", fmt.Errorf("empty PodIP for agentID %s", agentID)
	}

	// Exact match: replace own pod IP with loopback for local dispatch.
	// strings.Replace would corrupt IPs that share a prefix (e.g. 10.0.1.1 inside 10.0.1.10).
	if value.PodIP == GetPodIP() {
		podIP = d.loopback
	} else {
		podIP = value.PodIP
	}
	return podIP, nil
}

func (d *ipInventory) SetIP(ctx context.Context, agentID string, expiration time.Duration) error {
	return d.SetIPWithAgentInfo(ctx, agentID, nil, nil, expiration)
}

func (d *ipInventory) SetIPWithLabels(ctx context.Context, agentID string, labels map[string]string, expiration time.Duration) error {
	return d.SetIPWithAgentInfo(ctx, agentID, labels, nil, expiration)
}

func (d *ipInventory) SetIPWithAgentInfo(ctx context.Context, agentID string, labels map[string]string, capabilities []string, expiration time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()

	value := Value{
		PodIP:        GetPodIP(),
		Labels:       cloneStringMap(labels),
		Capabilities: cloneStringSlice(capabilities),
	}

	_, err := d.inventory.Set(ctx, d.getCacheKey(agentID), value, expiration)
	if err != nil {
		return err
	}
	if _, err := d.inventory.SAdd(ctx, d.activeAgentSetKey(), agentID); err != nil {
		return err
	}
	return nil
}

// DeleteIPs removes the agents' records that point at this pod. A record
// another pod wrote since (the agent reconnected there during a rolling
// restart) is kept: a late delete from the old pod would make the agent
// unreachable from every other replica until its next refresh.
func (d *ipInventory) DeleteIPs(ctx context.Context, agentIDs ...string) error {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	self := GetPodIP()
	mine := agentIDs[:0:0]
	for _, agentID := range agentIDs {
		// ponytail: get-then-delete, not atomic; a Lua compare-and-delete
		// closes the remaining microsecond window if it ever matters.
		if v, err := d.inventory.Get(ctx, d.getCacheKey(agentID)); err == nil && v != nil && v.PodIP != self {
			continue
		}
		mine = append(mine, agentID)
	}
	agentIDs = mine
	if len(agentIDs) == 0 {
		return nil
	}
	if len(agentIDs) > 0 {
		if _, err := d.inventory.SRem(ctx, d.activeAgentSetKey(), agentIDs...); err != nil {
			return err
		}
	}
	var keys []string
	for _, agentID := range agentIDs {
		keys = append(keys, d.getCacheKey(agentID))
	}
	if _, err := d.inventory.Del(ctx, keys...); err != nil {
		return err
	}
	return nil
}

// ListAgentIDs returns agent IDs with a live inventory record.
func (d *ipInventory) ListAgentIDs(ctx context.Context) ([]string, error) {
	records, err := d.ListAgentRecords(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(records))
	for _, rec := range records {
		out = append(out, rec.AgentID)
	}
	return out, nil
}

// ListAgentRecords returns currently active inventory records from Redis. The
// active set is intentionally only an index; the per-agent key remains the
// source of truth because it has the connection TTL and metadata.
func (d *ipInventory) ListAgentRecords(ctx context.Context) ([]AgentRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	agentIDs, err := d.inventory.SInter(ctx, d.activeAgentSetKey())
	if err != nil {
		return nil, err
	}
	records := make([]AgentRecord, 0, len(agentIDs))
	var stale []string
	for _, agentID := range agentIDs {
		value, err := d.inventory.Get(ctx, d.getCacheKey(agentID))
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				stale = append(stale, agentID)
				continue
			}
			return nil, err
		}
		if value == nil || value.PodIP == "" {
			stale = append(stale, agentID)
			continue
		}
		podIP := value.PodIP
		if podIP == GetPodIP() {
			podIP = d.loopback
		}
		records = append(records, AgentRecord{
			AgentID:      agentID,
			PodIP:        podIP,
			Labels:       cloneStringMap(value.Labels),
			Capabilities: cloneStringSlice(value.Capabilities),
		})
	}
	if len(stale) > 0 {
		if _, err := d.inventory.SRem(ctx, d.activeAgentSetKey(), stale...); err != nil {
			d.logger.Warn("failed to prune stale active inventory members", "count", len(stale), "error", err)
		}
	}
	return records, nil
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}

func cloneStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

// GetDispatcherCacheKey is the directory key of an agent's request
// (dispatcher) record.
func GetDispatcherCacheKey(agentID string) string {
	return "dipi:agentID:" + agentID
}

// GetStreamerCacheKey is the directory key of an agent's stream (streamer)
// record.
func GetStreamerCacheKey(agentID string) string {
	return "sipi:agentID:" + agentID
}

// GetPodIP is this replica's address: the environment variable pod_ip.env_var
// names ("" when unset).
func GetPodIP() string {
	return os.Getenv(viper.GetString("pod_ip.env_var"))
}
