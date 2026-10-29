// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/internal/platform/txnregistry"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay-core/platform/util"
	"github.com/nfltr/relay/client/handoff"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DispatcherConfig holds all configuration values that Dispatcher methods need.
// By injecting config at construction time instead of reading viper inline,
// the Dispatcher becomes unit-testable without global config state.
//
// Heartbeat Design (Bidirectional):
//   - Server sends heartbeats to agents every HeartbeatSendDuration (5s default),
//     skipping one right after other outbound data, via:
//   - sendGRPCHeartbeats(): Sends empty TunnelFrame to gRPC agent streams
//   - relayRequestsToAgent(): Sends empty HTTPProxyRequest to HTTP agents
//   - The accept headers state the interval (agentwire.HeartbeatKey); an agent
//     ends a connection silent for three of them (agentwire.SilenceLimit).
//   - Agents echo the heartbeats; the server ends an agent silent for
//     HeartbeatRecvThreshold (45s default) with errNoHeartbeat
//   - rpcclient agents also send their own heartbeats while idle, ahead of
//     large outbound payloads
//   - Last-seen and session-hook renewals (Renew) ride the heartbeat at most every
//     heartbeatBookkeepingEvery (15s)
type DispatcherConfig struct {
	ConnectedAgentsThreshold int
	HeartbeatRecvThreshold   time.Duration
	HeartbeatSendDuration    time.Duration
	HeartbeatEnabled         bool
	RecvBufferSize           int
	SyncMapClearTimeout      time.Duration
	GRPCPort                 int
	DropAgentOnDownstreamErr bool
	LoopbackAddr             string
	AdminMessage             string
	AdminMessageSet          bool
	// E2EEPolicy and E2EEDefault: tcp.e2ee_policy, tcp.e2ee_default
	// (connect_e2ee.go). An empty policy is allow.
	E2EEPolicy  string
	E2EEDefault bool
	// Uncounted names agents the connected-agent limit does not count
	// (and AgentCount does not report): the relay's own service agents.
	Uncounted func(agentID string) bool
	// Reserved are the names no agent may connect under (reserved_names.go):
	// the built-in product labels and the relay's site host labels.
	Reserved Reserved
}

// Dispatcher manages connected agent streams, routes proxied HTTP and gRPC
// requests to the correct agent, and handles heartbeat monitoring.
type Dispatcher struct {
	pb.UnimplementedDispatcherServer

	Agents                  *util.SyncMap[string, *HTTPStream]
	Upstreams               *util.SyncMap[string, Streamer]
	internalAgents          sync.Map // agent id → struct{}: RegisterInternalAgent
	drainedAgents           sync.Map // agent id → time.Time it left after draining (agent_drain.go)
	NumberOfAgentsConnected atomic.Int64
	// admitMu makes the connected-agent limit check and the admission it
	// grants one step; admitting counts the sessions of each agent id
	// admitted under the limit and not ended yet (validateAgentConnection).
	admitMu   sync.Mutex
	admitting map[string]int

	broker              broker.MessageBroker
	IPInventory         directory.IPInventory
	StreamerIPInventory directory.IPInventory

	Logger *slog.Logger

	podIDTopicProducer broker.Producer

	// TxnRegistry holds the in-flight RelayTunnel transactions, which an
	// agent's responses fan out to. Exported for core/session/sessiontest.
	TxnRegistry *txnregistry.TransactionRegistry[*pb.TunnelFrame]

	// a2aSessions tracks active agent-to-agent routing sessions keyed by
	// transaction ID. Each session has a request channel that the receive
	// loop feeds into; a background goroutine drains the channel and
	// forwards messages to the target agent.
	a2aSessions *util.SyncMap[string, *a2aSession]
	// handoffStall bounds an agent receive loop's wait on one of its
	// transactions (handoff.Stall; tests shorten it).
	handoffStall time.Duration
	// moveBound, rebalanceMinAge and rebalanceMovesPerTick: the move's and
	// the rebalance's bounds (agent_move.go, agent_rebalance.go), per
	// dispatcher so a test changes them for its own replicas only.
	moveBound             time.Duration
	rebalanceMinAge       time.Duration
	rebalanceMovesPerTick int

	Config DispatcherConfig

	globalContext           context.Context
	gracefulShutdownTimeout time.Duration
	// AgentsGo, set on a relay without peers, lets its agents go when it
	// closes (its clients drained, connect_drain.go) instead of at a
	// jittered moment of the shutdown.
	AgentsGo <-chan struct{}

	// OnAgentDisconnect is called with the agentID each time an agent stream closes.
	// Used to revoke share codes when their owning agent goes offline.
	OnAgentDisconnect func(agentID string)

	// OnAgentConnect is called with the agentID and optional TLS fingerprint
	// each time an agent stream opens.
	// Used to persist agent identity in the endpoint registry.
	OnAgentConnect func(agentID, tlsFingerprint string)

	// OnAgentMetadata is called with the advertised tunnel metadata each time an
	// agent stream opens. Used to persist route-catalog state in the endpoint registry.
	OnAgentMetadata func(info AgentInfo)

	// OnAgentHeartbeat is called when a transport keepalive confirms that an
	// agent stream is still alive. Used to refresh endpoint-registry last_seen_at.
	OnAgentHeartbeat func(agentID string)

	// DurableRegistration reports whether agentID already holds a registry
	// row not marked ephemeral; such an id cannot become ephemeral.
	// onEphemeralAgentGone removes an ephemeral agent's registry row once its
	// stream has closed. ephemeralMu orders that removal against a reconnect
	// of the same id registering it again (agent_lifetime.go).
	DurableRegistration  func(agentID string) bool
	OnEphemeralAgentGone func(agentID string)
	ephemeralMu          sync.Mutex

	// TCPDispatcher is the TCP tunneling engine (Gap 3).
	// Nil when tcp.enabled=false (default).
	TCPDispatcher *TCPDispatcher

	// PodDialer opens RelayTunnel streams to peer replicas on the replica
	// link (dialPeerStream); Replicas is the rest of the seam (replicas.go).
	PodDialer PodDialer
	Replicas
	// ClusterRelayPeers marks RelayTunnel callers as cluster-internal pods
	// even without a pod dialer (multi-pod tests simulating a peer pod).
	ClusterRelayPeers bool

	// SingleTenant makes every authenticated caller one tenant: set from
	// security.trust_loopback (DEV ONLY), as agentTenantScope treats it.
	SingleTenant bool

	// AgentOwners holds the owner of each agent connected here (its
	// principal's, recorded by registerAgent), so A2A routing reads it
	// without a store round trip; an agent elsewhere's is in its record
	// (ownerForAgent).
	AgentOwners *util.SyncMap[string, string]

	// AgentKeyBase caches agentID → key base ID (pre-scoping identity) so
	// A2A routing can resolve unscoped target names. When agent "key.worker"
	// sends to target "listener", the router prepends "key." to try
	// "key.listener" as a fallback. Populated during agent connect from
	// principal's subject; empty for mTLS agents (no scoping).
	AgentKeyBase *util.SyncMap[string, string]
	// agentStateMu orders registerAgent's writes of the state an agent's
	// sessions share (agentKeyBase, agentOwners) with forgetAgent's removal.
	agentStateMu sync.Mutex

	// SessionHook is a frontend's part in agent sessions (session_hook.go);
	// nil for none. Set before the dispatcher serves.
	SessionHook SessionHook

	// overShare: this replica holds more than its share of the fleet's
	// agents and refuses new agent sessions (agent_rebalance.go).
	overShare atomic.Bool
	// movedAway: agents whose moved session here no longer routes but
	// still ends (its settle, MOVE_DONE) and holds its subscription
	// (agent_rebalance.go refusesAgents).
	movedAway sync.Map // agent id -> *HTTPStream
	// stays: agents whose rebalancing move reached no other replica, left
	// alone for stayFor (agent_rebalance.go).
	stays sync.Map // agent id -> time.Time the move was given up

	// staleAgentReaperInterval is how often RunStaleAgentReaper scans upstreams.
	staleAgentReaperInterval time.Duration
	// staleAgentMaxIdle evicts upstream agents with no received messages longer than this.
	staleAgentMaxIdle time.Duration
	// staleReaperWake wakes RunStaleAgentReaper when a gRPC agent registers.
	staleReaperWake chan struct{}

	// lastSeenByAgentID records the last inbound message time per gRPC
	// upstream agent: an *atomic.Int64 per agent (agentSeenAt).
	lastSeenByAgentID sync.Map
	// InventoryRefreshByAgentID records the last Redis inventory TTL refresh
	// driven by inbound stream activity.
	InventoryRefreshByAgentID sync.Map
	// reannounce is closed when the shared store lost this replica's
	// records (inventory_epoch.go): every session rewrites its own.
	reannounceMu sync.Mutex
	reannounce   chan struct{}
	// RecordWriteFailed: a session's record write failed since the last
	// epoch tick, which then re-announces (inventory_epoch.go).
	RecordWriteFailed atomic.Bool
	// agentTunnelCancels holds per-agent cancel funcs for active AgentTunnel handlers.
	agentTunnelCancels sync.Map
	// agentTunnelMu makes an AgentTunnel's upstreams entry and its
	// agentTunnelCancels entry one step: two connects of an id racing
	// between them would leave the one routed cancelled and the other live.
	agentTunnelMu sync.Mutex

	UpstreamRegMu   sync.Mutex
	UpstreamRegCond *sync.Cond
	// Presence is the presence change feed (presence.go).
	Presence presenceFeed
}

// NewDispatcherConfig creates a DispatcherConfig from the current viper state.
// Call this once at startup and pass the result to NewDispatcher.
func NewDispatcherConfig() DispatcherConfig {
	return DispatcherConfig{
		ConnectedAgentsThreshold: viper.GetInt("rpc.connected_agents_threshold"),
		HeartbeatRecvThreshold:   viper.GetDuration("rpc.agent.heartbeat.recvthreshold"),
		HeartbeatSendDuration:    viper.GetDuration("rpc.agent.heartbeat.sendduration"),
		HeartbeatEnabled:         viper.GetBool("rpc.agent.heartbeat.enable"),
		RecvBufferSize:           viper.GetInt("rpc.recv_buffer_size"),
		SyncMapClearTimeout:      viper.GetDuration("grpc.proxying.downstream.sync_map.clear_timeout"),
		GRPCPort:                 viper.GetInt("grpc-port"),
		DropAgentOnDownstreamErr: viper.GetBool("on_downstream_errors.drop_agent_connection"),
		LoopbackAddr:             viper.GetString("loopback"),
		AdminMessage:             viper.GetString("admin.message"),
		AdminMessageSet:          true,
		E2EEPolicy:               viper.GetString("tcp.e2ee_policy"),
		E2EEDefault:              viper.GetBool("tcp.e2ee_default"),
	}
}

// NewDispatcher creates and starts a Dispatcher with the given broker, IP
// inventories, and optional DispatcherConfig override (defaults read from viper).
func NewDispatcher(globalContext context.Context, logger *slog.Logger, msgBroker broker.MessageBroker, podIDTopicProducer broker.Producer, dispatcherIPInventory, streamerIPInventory directory.IPInventory, gracefulShutdownTimeout time.Duration, cfg ...DispatcherConfig) *Dispatcher {
	var dcfg DispatcherConfig
	if len(cfg) > 0 {
		dcfg = cfg[0]
	} else {
		dcfg = NewDispatcherConfig()
	}
	if dcfg.LoopbackAddr == "" {
		dcfg.LoopbackAddr = viper.GetString("loopback")
	}
	if !dcfg.AdminMessageSet {
		dcfg.AdminMessage = viper.GetString("admin.message")
	}

	txnReg := txnregistry.NewTransactionRegistry[*pb.TunnelFrame]()
	txnReg.StartReaper(globalContext, 30*time.Second, logger, func(size int) {
		metrics.SetTxnRegistrySize(size)
	})

	d := &Dispatcher{
		Agents:              &util.SyncMap[string, *HTTPStream]{},
		Upstreams:           &util.SyncMap[string, Streamer]{},
		broker:              msgBroker,
		podIDTopicProducer:  podIDTopicProducer,
		IPInventory:         dispatcherIPInventory,
		StreamerIPInventory: streamerIPInventory,
		Logger:              logger,

		TxnRegistry: txnReg,

		a2aSessions:  &util.SyncMap[string, *a2aSession]{},
		handoffStall: handoff.Stall,
		AgentOwners:  &util.SyncMap[string, string]{},
		AgentKeyBase: &util.SyncMap[string, string]{},

		// PodDialer is set only in k8s-style multi-pod modes; nil otherwise.
		// All protocol handlers guard usage with if d.podDialer == nil checks.
		// Its links come with the replica link (startPeerLink); without
		// one, forwards to other replicas are refused.
		PodDialer: func() PodDialer {
			if config.IsK8sMultiPodEnv(viper.GetString("ENV")) {
				return offLinks{}
			}
			return nil
		}(),

		Config: dcfg,

		globalContext:           globalContext,
		gracefulShutdownTimeout: gracefulShutdownTimeout,

		moveBound:             defaultMoveBound,
		rebalanceMinAge:       defaultRebalanceMinAge,
		rebalanceMovesPerTick: defaultRebalanceMovesPerTick,

		staleAgentReaperInterval: 60 * time.Second,
		staleAgentMaxIdle:        180 * time.Second,
		staleReaperWake:          make(chan struct{}, 1),
	}
	d.UpstreamRegCond = sync.NewCond(&d.UpstreamRegMu)
	return d
}

// classifyStreamError maps errgroup errors to appropriate gRPC status codes.
func (d *Dispatcher) classifyStreamError(err error) error {
	switch {
	case errors.Is(err, errServerRestarting), errors.Is(err, errNoHeartbeat):
		return status.Errorf(codes.Canceled, "Canceled")
	case errors.Is(err, context.Canceled) && d.globalContext.Err() != nil:
		return status.Errorf(codes.Canceled, "Canceled")
	case errors.Is(err, context.Canceled):
		return nil
	default:
		return status.Errorf(codes.Canceled, "Canceled")
	}
}

func (d *Dispatcher) logStreamLifecycle(msg string, err error, attrs ...any) {
	logStreamLifecycleWithLogger(d.Logger, msg, err, attrs...)
}

func logStreamLifecycleWithLogger(logger *slog.Logger, msg string, err error, attrs ...any) {
	if logger == nil {
		return
	}
	attrs = append(attrs, "error", err)
	switch {
	case isExpectedStreamDisconnect(err):
		logger.Debug(msg, attrs...)
	case errors.Is(err, errNoHeartbeat):
		logger.Warn(msg, attrs...)
	default:
		logger.Error(msg, attrs...)
	}
}

func isExpectedStreamDisconnect(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	if status.Code(err) == codes.Canceled {
		return true
	}
	// gRPC may surface a normal bidirectional stream teardown as
	// Unavailable/transport-is-closing, and some older relay paths wrap
	// that status with %v instead of %w. Treat only this exact transport
	// close signature as expected so real Unavailable routing failures
	// still remain visible.
	return strings.Contains(err.Error(), "transport is closing")
}

// validateAgentConnection checks whether an agent connection can be accepted:
//  1. Verifies the agentID is not already connected via the given inventory
//     (ConnectHTTP first lets a reconnect take its own live session over:
//     takeOverSession).
//  2. Checks that the relay's connected-agent limit (0 = none) is not
//     reached: FailedPrecondition, which agents do not retry (they stop and
//     print the message, as for a replaced session). The check and the
//     admission are one step: an admitted id counts against the limit from
//     then until its session calls release, so concurrent connects cannot
//     all pass the check before any of them registers.
//
// One API key may run any number of sessions under distinct agent IDs.
//
// When the inventory shows the agent on this pod but no local stream exists
// (stale Redis entry after an ungraceful disconnect), the stale entry is
// cleaned up and the reconnection is allowed immediately.
//
// On success the caller defers release, which is a no-op without a limit.
func (d *Dispatcher) validateAgentConnection(ctx context.Context, agentID, credential string, inv directory.IPInventory) (release func(), err error) {
	podIP, err := inv.GetIP(ctx, agentID)
	if err == nil && podIP != "" && podIP != d.Config.LoopbackAddr && d.movedFromPod(ctx, agentID, credential, inv) {
		d.Logger.Info("agent reconnected here while another pod still holds its old session: this one supersedes it", "AgentID", agentID, "ip", podIP)
		podIP = ""
	}
	if err == nil && podIP != "" {
		// The inventory thinks this agent is already connected.
		// If the record points to this pod (loopback), verify the stream
		// is actually alive in the local agents map. A stale entry can
		// linger after an ungraceful disconnect (SIGKILL, network drop)
		// until the Redis TTL expires.
		if podIP == d.Config.LoopbackAddr {
			if _, live := d.Agents.Load(agentID); !live {
				d.Logger.Info("cleaning up stale inventory entry for reconnecting agent",
					"AgentID", agentID)
				_ = inv.DeleteIPs(ctx, agentID)
				// Fall through — allow the new connection.
			} else {
				d.Logger.Info("agent already connected to this pod", "AgentID", agentID)
				return nil, status.Errorf(codes.AlreadyExists, "Agent for AgentID %s is already connected", agentID)
			}
		} else {
			// Another credential (or a record without one): retryable on
			// the client, the other pod's entry expires with its session.
			d.Logger.Info("agent already connected to another pod", "AgentID", agentID, "ip", podIP)
			return nil, status.Errorf(codes.AlreadyExists, "Agent for AgentID %s is already connected", agentID)
		}
	}

	// The count walks every session: count only when a limit is set.
	limit := d.Config.ConnectedAgentsThreshold
	if limit <= 0 || d.uncounted(agentID) {
		return func() {}, nil
	}
	// One lock over count and admission; taken only when a limit is set.
	d.admitMu.Lock()
	defer d.admitMu.Unlock()
	ids := d.agentIDSet()
	for id := range d.admitting {
		ids[id] = struct{}{}
	}
	if connectedAgents := len(ids); connectedAgents >= limit {
		d.Logger.Warn("refusing agent: connected-agent limit reached", "AgentID", agentID, "count", connectedAgents, "limit", limit)
		// FailedPrecondition: agents stop and print this instead of retrying.
		return nil, status.Errorf(codes.FailedPrecondition, "relay is at its limit of %d connected agents; its operator can raise it (nfltr relay serve --max-agents, rpc.connected_agents_threshold; 0 = no limit)", limit)
	}
	if d.admitting == nil {
		d.admitting = map[string]int{}
	}
	d.admitting[agentID]++
	var once sync.Once
	return func() {
		once.Do(func() {
			d.admitMu.Lock()
			defer d.admitMu.Unlock()
			if d.admitting[agentID]--; d.admitting[agentID] == 0 {
				delete(d.admitting, agentID)
			}
		})
	}, nil
}

// hashAPIKey returns the hex-encoded SHA-256 hash of the raw API key.
func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// sessionCredential tags the credential a session connected with in its
// shared inventory record (directory.Session.Credential): derived from the
// key's hash, not the hash itself; "" without an API key.
func sessionCredential(apiKeyHash string) string {
	if apiKeyHash == "" {
		return ""
	}
	return hashAPIKey("nfltr-session-owner:" + apiKeyHash)[:32]
}

// streamCredential is sessionCredential for the API key a stream connected
// with.
func streamCredential(ctx context.Context) string {
	if key := firstIncoming(ctx, "x-api-key"); key != "" {
		return sessionCredential(hashAPIKey(key))
	}
	return ""
}

// ---------------------------------------------------------------------------
// Shared agent-lifecycle helpers
// ---------------------------------------------------------------------------

// newHeartbeatState creates the heartbeat channel and recv-threshold ticker
// shared by both HTTP and gRPC agent connections.
func newHeartbeatState(recvThreshold time.Duration) (heartbeats chan proto.Message, ticker *time.Ticker) {
	return make(chan proto.Message, 1), time.NewTicker(recvThreshold)
}

// RegisterAgent counts a new session of agentID and records the state the
// agent's sessions share: its key base (its principal's subject, "" none)
// and owner (its principal's owner, "" none: an in-process agent).
// A session calls it only once it holds the agent (stored in agents or
// upstreams), so the teardown of a session it replaced, which runs
// forgetAgent, leaves what it records.
func (d *Dispatcher) RegisterAgent(agentID, keyBase, owner, tlsFingerprint string) {
	d.NumberOfAgentsConnected.Add(1)
	d.Logger.Debug("number of agents connected", "AgentID", agentID, "count", d.NumberOfAgentsConnected.Load())
	d.agentStateMu.Lock()
	if keyBase != "" {
		d.AgentKeyBase.Store(agentID, keyBase)
	}
	if owner != "" {
		d.AgentOwners.Store(agentID, owner)
	}
	d.agentStateMu.Unlock()
	if d.OnAgentConnect != nil {
		d.OnAgentConnect(agentID, tlsFingerprint)
	}
}

// DeregisterAgent ends ConnectHTTP session s of agentID: it uncounts the
// session, logs why it ended and removes it from agents. Only the session
// agents still holds goes on to forgetAgent; a replaced session's teardown
// touches nothing of the session that replaced it.
func (d *Dispatcher) DeregisterAgent(agentID string, s *HTTPStream, streamCtxErr error) {
	if streamCtxErr != nil {
		d.logStreamLifecycle("agent disconnected", streamCtxErr, "AgentID", agentID)
	} else {
		d.Logger.Info("agent disconnected with no error", "AgentID", agentID)
	}
	d.NumberOfAgentsConnected.Add(-1)
	if !d.Agents.CompareAndDelete(agentID, s) {
		d.Logger.Info("skipping stale cleanup for replaced agent session", "AgentID", agentID)
		return
	}
	d.forgetAgent(agentID)
}

// forgetAgent removes the state an agent's sessions share (key base, owner,
// inventory refresh) and runs onAgentDisconnect, once no session of agentID
// is registered here: an agent's ConnectHTTP and AgentTunnel sessions share
// it, so the end of one leaves it to the other, and a replaced session's
// end leaves it to its replacement.
func (d *Dispatcher) forgetAgent(agentID string) {
	d.agentStateMu.Lock()
	_, httpLive := d.Agents.Load(agentID)
	_, tunnelLive := d.Upstreams.Load(agentID)
	if httpLive || tunnelLive {
		d.agentStateMu.Unlock()
		return
	}
	d.InventoryRefreshByAgentID.Delete(agentID)
	d.AgentOwners.Delete(agentID)
	d.AgentKeyBase.Delete(agentID)
	d.agentStateMu.Unlock()
	if d.OnAgentDisconnect != nil {
		d.OnAgentDisconnect(agentID)
	}
}

// UpdateAgentLabels sets the labels a connected gRPC agent advertises after
// connect (nfltr.presence.v1 UpdateLabels, sent by the agent itself): the
// update replaces the previous update's labels, or with patch merges into
// them; the labels the agent connected with always stay as advertised (an
// update can neither change nor drop them). It returns the full set now
// advertised, or ok=false for an agent not connected here.
func (d *Dispatcher) UpdateAgentLabels(agentID string, labels map[string]string, patch bool) (map[string]string, bool) {
	stream, ok := d.Upstreams.Load(strings.TrimSpace(agentID))
	if !ok {
		return nil, false
	}
	grpc, ok := stream.(*GRPCStream)
	if !ok {
		return nil, false
	}
	next := map[string]string{}
	if patch {
		maps.Copy(next, grpc.Labels())
	}
	for k, v := range labels {
		if k, v = strings.TrimSpace(k), strings.TrimSpace(v); k != "" && v != "" {
			next[k] = v
		}
	}
	maps.Copy(next, grpc.connectLabels)
	grpc.SetLabels(next)
	if err := d.StreamerIPInventory.SetIPWithAgentInfo(context.Background(), agentID, next, grpc.Capabilities(), d.Config.HeartbeatRecvThreshold); err != nil {
		d.Logger.Warn("label update: redis inventory failed", "agent_id", agentID, "error", err)
	}
	d.Presence.labels(agentID, next)
	return maps.Clone(next), true
}

// DeregisterUpstreamAgent removes agentStream, which ended with
// streamCtxErr, as agentID's session; false when a newer session of the
// agent replaced it (that one stays).
func (d *Dispatcher) DeregisterUpstreamAgent(agentID string, agentStream *GRPCStream, streamCtxErr error) bool {
	if streamCtxErr != nil {
		d.logStreamLifecycle("agent disconnected", streamCtxErr, "AgentID", agentID)
	} else {
		d.Logger.Info("agent disconnected with no error", "AgentID", agentID)
	}
	d.NumberOfAgentsConnected.Add(-1)

	if !d.Upstreams.CompareAndDelete(agentID, agentStream) {
		if _, ok := d.Upstreams.Load(agentID); ok {
			d.Logger.Info("skipping stale upstream cleanup for replaced agent session", "AgentID", agentID)
			return false
		}
	}
	d.forgetAgent(agentID)
	return true
}

// publishDeparture tells presence watchers an agent left. Callers publish
// only once the agent is out of everything /api/v1/my/agents lists
// (upstreams and the streamer inventory): a watcher woken earlier re-read the
// departed agent as connected (graceful node stop under load, 2026-10-02).
func (d *Dispatcher) publishDeparture(agentID string, _ Streamer) {
	d.Presence.down(agentID)
}

// agentHeartbeat is the heartbeat interval of the agent stream whose
// context is ctx: the one the agent asks for (agentwire.HeartbeatAskKey,
// its liveness preset) kept within [agentwire.MinLivenessInterval, the
// smaller of agentwire.MaxLivenessInterval and a third of the receive
// threshold, since the agent answers each heartbeat], else the configured
// rpc.agent.heartbeat.sendduration.
func (d *Dispatcher) agentHeartbeat(ctx context.Context) time.Duration {
	if ctx == nil { // a stream built without one (tests)
		return d.Config.HeartbeatSendDuration
	}
	ask := agentwire.Heartbeat(incomingKeys(ctx, agentwire.HeartbeatAskKey), agentwire.HeartbeatAskKey)
	if ask <= 0 {
		return d.Config.HeartbeatSendDuration
	}
	hi := agentwire.MaxLivenessInterval
	if third := d.Config.HeartbeatRecvThreshold / 3; third > 0 {
		hi = max(agentwire.MinLivenessInterval, min(hi, third))
	}
	return min(max(ask, agentwire.MinLivenessInterval), hi)
}

// appendHeartbeat states the interval the relay keeps on an agent stream in
// its accept headers (agentwire.HeartbeatKey): the agent ends a connection
// silent for its liveness's wait past it (agentwire.Liveness.DeadAfter), so a
// relay that died without a FIN reaching the agent is noticed within
// seconds rather than the transport keepalive's tens of seconds.
func (d *Dispatcher) appendHeartbeat(md metadata.MD, interval time.Duration) {
	if !d.Config.HeartbeatEnabled {
		return
	}
	if kv := agentwire.HeartbeatPairs(agentwire.HeartbeatKey, interval); kv != nil {
		md.Append(kv[0], kv[1])
	}
}

// heartbeatBookkeepingEvery bounds how often a heartbeat refreshes the
// agent's last-seen record (a SQL write) and its session hook's renewal:
// the heartbeat runs every few seconds for the agents' liveness, the
// records need no more than the 15s cadence they had.
const heartbeatBookkeepingEvery = 15 * time.Second

// NotifyAgentHeartbeat records a heartbeat of agentID (OnAgentHeartbeat),
// except for an agent on its agent link, whose last-seen is recorded when it
// disconnects.
func (d *Dispatcher) NotifyAgentHeartbeat(agentID string) {
	// An agent on its agent link: its last-seen is written when it
	// disconnects (OnAgentDisconnect), not every heartbeat.
	if d.OnAgentHeartbeat != nil && !d.TCPDispatcher.LinkUp(agentID) {
		d.OnAgentHeartbeat(agentID)
	}
}

// bookkeepHeartbeat runs NotifyAgentHeartbeat off the caller's loop, one at
// a time per loop (busy): its write (the endpoint registry's last_seen_at)
// may hang, as on a replica healing from a partition, and the loop forwards
// the agent's requests (soak 2026-10-26: 10-22 s late answers;
// TestRelayRequestsToAgentNotHeldByHeartbeatBookkeeping).
func (d *Dispatcher) bookkeepHeartbeat(agentID string, busy *atomic.Bool) {
	if !busy.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer busy.Store(false)
		d.NotifyAgentHeartbeat(agentID)
	}()
}

func (d *Dispatcher) markAgentStreamAlive(tracker HeartbeatTracker, msg proto.Message) {
	select {
	case tracker.Heartbeats() <- msg:
	default:
	}
}

// onHeartbeatSendError handles the common error path when a heartbeat send fails:
// it logs the send error and removes the agent's IP from the inventory.
func (d *Dispatcher) onHeartbeatSendError(agentID string, sendErr error, ipInventory directory.IPInventory) {
	d.Logger.Error("failed to send heartbeat to agent", "AgentID", agentID, "error", sendErr)
	metrics.RegisterUpstreamHeartbeatFailure("send_failure")
	cacheErr := ipInventory.DeleteIPs(context.Background(), agentID)
	if cacheErr != nil {
		d.Logger.Error("failed to delete agentID dispatcher addr from cache", "AgentID", agentID, "error", cacheErr)
	}
}

// ---------------------------------------------------------------------------

// IsAgentConnected reports whether the agent with the given ID has an active
// HTTP stream registered with this Dispatcher.
func (d *Dispatcher) IsAgentConnected(agentID string) bool {
	_, ok := d.Agents.Load(agentID)
	return ok
}

// GetAllConnectedAgents returns a snapshot list of all currently connected
// agent IDs registered with this Dispatcher.
func (d *Dispatcher) GetAllConnectedAgents() []string {
	agentIDs := make([]string, 0)
	d.Agents.Range(func(key string, _ *HTTPStream) bool {
		agentIDs = append(agentIDs, key)
		return true
	})
	return agentIDs
}

// AgentInfo holds metadata for a connected agent.
type AgentInfo struct {
	AgentID      string            `json:"agent_id"`
	TunnelType   string            `json:"tunnel_type"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Routes       []string          `json:"routes,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	ConnectedAt  time.Time         `json:"connected_at"`
	BytesSent    int64             `json:"bytes_sent"`
	BytesRecv    int64             `json:"bytes_recv"`
	RequestCount int64             `json:"request_count"`
	Client       agentwire.Client  `json:"client,omitzero"`
}

// GetAllAgentInfo returns metadata and stats for every connected HTTP agent.
func (d *Dispatcher) GetAllAgentInfo() []AgentInfo {
	infos := make([]AgentInfo, 0)
	d.Agents.Range(func(key string, s *HTTPStream) bool {
		caps := s.Capabilities()
		if len(caps) == 0 {
			caps = []string{"browse"}
		}
		infos = append(infos, AgentInfo{
			AgentID:      key,
			TunnelType:   ProtocolHTTP,
			Capabilities: caps,
			Routes:       s.Routes(),
			Labels:       s.Labels(),
			ConnectedAt:  s.ConnectedAt(),
			BytesSent:    s.BytesSent(),
			BytesRecv:    s.BytesRecv(),
			RequestCount: s.RequestCount(),
			Client:       s.Client(),
		})
		return true
	})
	return infos
}

// GetAllUpstreamAgentInfo returns metadata for every connected gRPC tunnel agent.
func (d *Dispatcher) GetAllUpstreamAgentInfo() []AgentInfo {
	var infos []AgentInfo
	d.Upstreams.Range(func(key string, s Streamer) bool {
		info := AgentInfo{
			AgentID:    key,
			TunnelType: "grpc",
		}
		if ls, ok := s.(interface{ Labels() map[string]string }); ok {
			info.Labels = ls.Labels()
		}
		if cs, ok := s.(interface{ Capabilities() []string }); ok {
			info.Capabilities = cs.Capabilities()
		}
		if cs, ok := s.(interface{ ConnectedAt() time.Time }); ok {
			info.ConnectedAt = cs.ConnectedAt()
		}
		if bs, ok := s.(interface{ BytesSent() int64 }); ok {
			info.BytesSent = bs.BytesSent()
		}
		if br, ok := s.(interface{ BytesRecv() int64 }); ok {
			info.BytesRecv = br.BytesRecv()
		}
		if rc, ok := s.(interface{ RequestCount() int64 }); ok {
			info.RequestCount = rc.RequestCount()
		}
		if c, ok := s.(interface{ Client() agentwire.Client }); ok {
			info.Client = c.Client()
		}
		infos = append(infos, info)
		return true
	})
	return infos
}

// GetTCPAgentAgentIDs returns the agentIDs of all agents with active TCP streams.
func (d *Dispatcher) GetTCPAgentAgentIDs() []string {
	if d.TCPDispatcher == nil {
		return nil
	}
	return d.TCPDispatcher.ConnectedAgentIDs()
}

// AgentCount returns the number of currently connected agents, not counting
// relay plumbing (IsInternalAgent).
func (d *Dispatcher) AgentCount() int64 {
	if d == nil {
		return 0
	}
	return int64(len(d.agentIDSet()))
}

// uncounted reports whether the agent limit and AgentCount skip agentID.
func (d *Dispatcher) uncounted(agentID string) bool {
	return d.Config.Uncounted != nil && d.Config.Uncounted(agentID)
}

// agentIDSet is the set AgentCount counts.
func (d *Dispatcher) agentIDSet() map[string]struct{} {
	seen := map[string]struct{}{}
	d.Agents.Range(func(agentID string, _ *HTTPStream) bool {
		if agentID != "" {
			seen[agentID] = struct{}{}
		}
		return true
	})
	d.Upstreams.Range(func(agentID string, _ Streamer) bool {
		if agentID != "" && !d.IsInternalAgent(agentID) {
			seen[agentID] = struct{}{}
		}
		return true
	})
	for _, agentID := range d.GetTCPAgentAgentIDs() {
		if agentID != "" {
			seen[agentID] = struct{}{}
		}
	}
	if d.Config.Uncounted != nil {
		for id := range seen {
			if d.Config.Uncounted(id) {
				delete(seen, id)
			}
		}
	}
	return seen
}

// movedFromPod reports whether agentID's record on another pod names a
// session of the same credential: the agent itself redialled here (its old
// replica died, or a partition cut it off and has not noticed yet), so the
// new session supersedes the old one instead of waiting, refused, until the
// old record expires (directory.SessionStore).
func (d *Dispatcher) movedFromPod(ctx context.Context, agentID, credential string, inv directory.IPInventory) bool {
	store, ok := inv.(directory.SessionStore)
	if !ok || credential == "" {
		return false
	}
	_, s, err := store.GetSession(ctx, agentID)
	return err == nil && s.Credential == credential
}
