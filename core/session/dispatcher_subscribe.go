// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/internal/platform/auth"
	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay-core/internal/platform/receiveloop"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/nfltr/relay/proto/wire/chunking"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ConnectHTTP implements the gRPC Dispatcher_ConnectHTTPServer handler, registering
// an HTTP-based agent stream and relaying frames until the agent disconnects.
func (d *Dispatcher) ConnectHTTP(stream pb.Dispatcher_ConnectHTTPServer) error {
	// Get AgentID from context — supports API key or mTLS auth.
	agentID, err := auth.GetAgentIDFromContext(stream.Context())
	if err != nil {
		d.Logger.Error("error determining agentID from context", "error", err)
		return status.Error(codes.PermissionDenied, "Error determining agentID from context")
	}
	if err := d.Config.Reserved.RefuseAgentName(agentID); err != nil {
		return err
	}

	// The API key identifies the principal allowed to take this session over.
	var apiKeyHash, keyBase string
	if key := firstIncoming(stream.Context(), "x-api-key"); key != "" {
		apiKeyHash = hashAPIKey(key)
	}
	// The pre-scoped key identity, for A2A target resolution and
	// ownership; registerAgent records it once this session holds the
	// agent (a session it takes over removes the agent's on its way out).
	keyBase = keyBaseOf(stream.Context())

	sessionCtx, cancel := context.WithCancelCause(stream.Context()) // will cancel if `stream context` cancels
	defer cancel(nil)
	ctx := sessionCtx
	registrationCtx := d.agentInventoryRegistrationContext(ctx)

	d.Logger.Info("received SubscribeEvent from agent for AgentID", "agent_id", agentID)
	if d.refusesAgents(registrationCtx, agentID) { // agent_move.go, agent_rebalance.go: its reconnect must reach another replica
		return status.Error(codes.Unavailable, "this relay replica is leaving or holds more than its share of agents: connect again")
	}
	if err := d.takeOverSession(registrationCtx, agentID, apiKeyHash); err != nil {
		return err
	}
	credential := sessionCredential(apiKeyHash)
	release, err := d.validateAgentConnection(registrationCtx, agentID, credential, d.IPInventory)
	if err != nil {
		return err
	}
	defer release()
	// The session leaves agents, and ended closes, after every other
	// deferred teardown step: a reconnect waits for ended (takeOverSession)
	// and registers only then, so no step here undoes its registration.
	ended := make(chan struct{})
	var registered *HTTPStream
	defer func() {
		if registered != nil {
			d.DeregisterAgent(agentID, registered, stream.Context().Err())
		}
		close(ended)
	}()

	// Send the server-resolved agent identity back so the agent can
	// display the correct browse URL (the server may have scoped --name
	// to the key's stored identity).
	md := metadata.Pairs("x-resolved-agent-id", agentID)
	md.Append(chunking.FlowControlMetadataKey, "1") // this relay sends cancel/ack frames
	if msg := d.Config.AdminMessage; msg != "" {
		md.Append("x-admin-message", msg)
	}
	// A frontend's part in the session (SessionHook): it may add to the
	// accept or refuse the session.
	var hooked HookedSession
	if h := d.SessionHook; h != nil {
		if hooked, err = h.Join(stream.Context(), agentID, keyBase, md); err != nil {
			return err
		}
	}
	services, err := declaredServices(stream.Context())
	if err != nil {
		return err
	}
	// An agent that does not read modes (nfltr before v2.1.6) requires
	// the echo to equal its declaration and cannot terminate E2EE on a tcp
	// service: it gets its declaration back, its tcp services plain.
	echo, modes := services, firstIncoming(stream.Context(), agentservices.ModesHeader) == "1"
	if !modes {
		if services, err = plainServices(services, d.Config.E2EEPolicy); err != nil {
			// Such an agent reads only the header of a refused stream:
			// accept its declaration there with the reason as the notice
			// it prints, then refuse (a status it stops on, not retried).
			md.Append(agentservices.Header, agentservices.Format(echo))
			md.Set("x-admin-message", status.Convert(err).Message())
			_ = stream.SendHeader(md)
			return err
		}
	}
	if err := CheckServicesE2EE(services, d.Config.E2EEPolicy); err != nil {
		return err
	}
	services = ResolveServicesE2EE(services, d.Config.E2EEPolicy, d.Config.E2EEDefault)
	if modes {
		echo = services
	}
	if services != nil {
		// Accepted: the agent may serve them, each tcp service's E2EE
		// resolved (its own choice, else the policy's or the default).
		md.Append(agentservices.Header, agentservices.Format(echo))
	}
	d.appendHeartbeat(md, d.agentHeartbeat(stream.Context()))
	if d.Config.HeartbeatEnabled && d.TCPDispatcher != nil && d.TCPDispatcher.Link != nil {
		// While its agent link is up, the link's liveness is the agent's:
		// it need not answer the heartbeats (WatchAgentSession).
		md.Append(agentwire.LinkLivenessKey, "1")
	}

	metrics.RegisterUpstreamConnection(ProtocolHTTP)
	upstreamStart := time.Now()

	agentIDTopicName := agentID
	err = d.broker.CreateTopic(agentIDTopicName, 0, true)
	if err != nil {
		d.Logger.Error("error creating topic for AgentID", "agent_id", agentID, "error", err)
		return status.Errorf(codes.Internal, "Error creating topic for AgentID %s", agentID)
	}

	recvThreshold := d.Config.HeartbeatRecvThreshold

	session, err := claimAgentRecord(registrationCtx, d.IPInventory, agentID, directory.Session{Since: time.Now(), Credential: credential, Account: PrincipalOwner(stream.Context()), Services: agentservices.Format(services)}, nil, nil, recvThreshold)
	if err != nil {
		d.Logger.Error("failed to set agentID dispatcher addr in cache", "agent_id", agentID, "error", err)
		return err
	}

	defer func() {
		err = d.IPInventory.DeleteIPs(context.Background(), agentID)
		if err != nil {
			d.Logger.Error("failed to delete agentID dispatcher addr from cache", "agent_id", agentID, "error", err)
		}
	}()

	heartbeats, heartbeatsRecvThresholdTicker := newHeartbeatState(recvThreshold)
	defer heartbeatsRecvThresholdTicker.Stop()

	agentStream := NewHTTPStream(agentID, stream, stream.Context(), heartbeats, heartbeatsRecvThresholdTicker)
	agentStream.connectedAt, agentStream.seq, agentStream.credential, agentStream.account = session.Since, session.Seq, session.Credential, session.Account
	agentStream.Services = services
	defer d.failInflight(agentStream) // the session ended: its unanswered transactions get agent-gone
	if services != nil {
		d.Logger.Info("agent declared services", "agent_id", agentID, "services", agentservices.Format(services))
	}

	// Extract advertised routes from gRPC metadata (e.g., "fossil,gitea").
	{
		md := incomingKeys(stream.Context(), "x-routes", agentwire.LabelsKey, "x-agent-capabilities",
			chunking.FlowControlMetadataKey, "x-tls-fingerprint")
		if vals := md.Get("x-routes"); len(vals) > 0 && vals[0] != "" {
			for r := range strings.SplitSeq(vals[0], ",") {
				r = strings.TrimSpace(r)
				if r != "" {
					agentStream.routes = append(agentStream.routes, r)
				}
			}
			d.Logger.Info("agent advertised routes", "agent_id", agentID, "routes", agentStream.routes)
		}
		agentStream.labels = parseLabelsMetadata(md)
		if len(agentStream.labels) > 0 {
			d.Logger.Info("agent advertised labels", "agent_id", agentID, "labels", agentStream.labels)
		}
		agentStream.capabilities = parseCapabilitiesMetadata(md)
		if len(agentStream.capabilities) > 0 {
			d.Logger.Info("agent advertised capabilities", "agent_id", agentID, "capabilities", agentStream.capabilities)
		}
		if advertisesMove(stream.Context()) {
			agentStream.moved, agentStream.rebalance = make(chan struct{}), make(chan struct{})
		}
		// E2EE: read TLS certificate fingerprint from agent metadata.
		if vals := md.Get("x-tls-fingerprint"); len(vals) > 0 && vals[0] != "" {
			agentStream.tlsFingerprint = vals[0]
			d.Logger.Info("agent advertised TLS fingerprint", "agent_id", agentID, "fingerprint", vals[0])
		}
	}

	// Subscribed (and receiving) before the session registers: a request
	// routed here once the agent counts as connected finds its consumer.
	// The in-memory broker drops, without an error, what no receiving
	// consumer takes, and its visitor waited for the request timeout.
	consumer, err := d.broker.Subscribe(broker.ConsumerOptions{
		Topic:            agentIDTopicName,
		SubscriptionName: agentID,
		Type:             broker.ConsumerTypeExclusive,
	})
	if err != nil {
		d.Logger.Error("failed to subscribe to topic", "topic", agentIDTopicName, "error", err)
		return err
	}
	defer func() {
		if err := consumer.Unsubscribe(); err != nil {
			d.Logger.Error("failed to unsubscribe from topic", "topic", agentIDTopicName, "error", err)
		}
		// Closed, it takes no more (a later send fails: the proxy answers
		// its visitor); what it took and the agent never got is answered.
		consumer.Close()
		d.failQueued(agentStream, consumer.Chan())
		d.movedAway.CompareAndDelete(agentID, agentStream)
	}()
	consumer.Chan() // receiving from here

	agentStream.takeover = &sessionTakeover{keyHash: apiKeyHash, replace: func() { cancel(errSessionReplaced) }, ended: ended}
	d.Agents.Store(agentID, agentStream)
	if d.OnAgentMetadata != nil {
		caps := agentStream.capabilities
		if len(caps) == 0 {
			caps = []string{"browse"}
		}
		d.OnAgentMetadata(AgentInfo{
			AgentID:      agentID,
			TunnelType:   ProtocolHTTP,
			Capabilities: cloneStringSlice(caps),
			Routes:       cloneStringSlice(agentStream.routes),
			Labels:       cloneStringMap(agentStream.labels),
			ConnectedAt:  agentStream.connectedAt,
		})
	}
	d.RegisterAgent(agentID, keyBase, agentStream.account, agentStream.tlsFingerprint)
	registered = agentStream // its teardown: the deferred close(ended) above
	// The accept goes out only now: an agent that hears it is connected
	// (a share for a declared service, at once) finds itself registered.
	if err := stream.SendHeader(md); err != nil {
		d.Logger.Warn("failed to send resolved agent-id header", "error", err)
	}

	// The hook's part starts before the receive loop below, so none of the
	// session's frames arrive before it is ready.
	if hooked != nil {
		if err := hooked.Start(agentStream.Send, func() { cancel(nil) }); err != nil {
			return err
		}
		agentStream.hooked = hooked
		defer hooked.Close()
	}

	eg, ctx := errgroup.WithContext(ctx)
	// Recv returns only when the gRPC stream ends, and the stream ends only
	// when this handler returns. The receive loop therefore runs outside the
	// group: a session ended from here (replaced, heartbeat timeout, relay
	// stop) returns without waiting for a Recv the client may never answer.
	received := make(chan error, 1) // watchAgentSession ends the session with it
	go func() {
		// publishes to the podID topic
		received <- d.receiveFromHTTPAgent(ctx, agentStream)
	}()
	eg.Go(func() error { return d.relayRequestsToAgent(ctx, agentStream, consumer) })
	err = d.runSession(eg, ctx, cancel, agentID, agentStream, d.IPInventory, received)

	// Record upstream connection duration with disconnect reason.
	disconnectReason := classifyDisconnectReason(err)
	metrics.RegisterUpstreamConnectionDuration(disconnectReason, time.Since(upstreamStart).Seconds())

	if errors.Is(context.Cause(sessionCtx), errSessionReplaced) {
		d.Logger.Info("agent session replaced by its reconnect", "agent_id", agentID)
		return errReplacedSessionStatus
	}
	if err != nil {
		d.logStreamLifecycle("agent for AgentID subscription is disconnected", err, "agent_id", agentID)
	}
	return d.classifyStreamError(err)
}

// send receives the requests from requests from http server via pulsar topic of the agentID and
// sends them to the grpc agent for the agentID.
func (d *Dispatcher) relayRequestsToAgent(ctx context.Context, stream *HTTPStream, consumer broker.Consumer) error {
	agentID := stream.ID
	logger := d.Logger.With("AgentID", agentID)
	interval := d.agentHeartbeat(stream.streamCtx)
	heartbeatTicker := time.NewTicker(interval)
	defer heartbeatTicker.Stop()
	var bookkept time.Time
	var bookkeeping atomic.Bool
	incomingMessages := consumer.Chan()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			if stream.OutboundDataSentWithin(interval) {
				logger.Debug("skipping heartbeat to agentID agent: outbound data recently sent")
				continue
			}
			if err := stream.Send(&pb.HTTPProxyRequest{}); err != nil {
				d.onHeartbeatSendError(agentID, err, d.IPInventory)
				return err
			}
			logger.Debug("sent heartbeat to agentID agent")
			if time.Since(bookkept) < heartbeatBookkeepingEvery {
				continue
			}
			bookkept = time.Now()
			if h := stream.hooked; h != nil {
				h.Renew()
			}
			d.bookkeepHeartbeat(agentID, &bookkeeping)
		case message, ok := <-incomingMessages:
			if !ok {
				return errors.New("broker consumer channel closed")
			}
			logger.Debug("received message from broker")
			now := time.Now()
			request, err := broker.DecodeBrokerMessage(message, &pb.HTTPProxyRequest{})
			if err != nil {
				metrics.RegisterBrokerMessageDecodeError()
				// Nack so the message can be redelivered
				return err
			}
			trackRequest(stream, request.Request)
			logger2 := logging.With(logger, "transaction_id", request.Request.TransactionId)
			logger2.Debug("unmarshalling message took", "duration", time.Since(now))
			logger2.Debug("time since message has left proxy server", "duration", now.Sub(time.UnixMilli(request.Timestamps.RequestAtHttpServer)))
			metrics.IncrDequeuedRequests()
			request.Timestamps.RequestAtGrpcServer = now.UnixMilli()
			err = stream.Send(request)
			if err != nil {
				logger2.Error("sending request to agent", "error", err)
				return fmt.Errorf("send failed: %w", err)
			}
			stream.bytesSent.Add(int64(len(request.Request.Payload)))
			stream.requestCount.Add(1)

			// Ack only after successful decode and send
			ackErr := message.Ack()
			if ackErr != nil {
				metrics.RegisterBrokerAckFailure()
				return fmt.Errorf("acking failed: %s: %w", message, ackErr)
			}

			logger2.Debug("handed to agent stream")
			metrics.RegisterRequestsSentToAgents()
		}
	}
}

// receive receives from grpc agent and
// loads the response from agent to a shared podID pulsar topic so the http server can consume.
func (d *Dispatcher) receiveFromHTTPAgent(ctx context.Context, stream *HTTPStream) error {
	recvThreshold := d.Config.HeartbeatRecvThreshold
	return receiveloop.ReceiveAndProcess(ctx, d.Config.RecvBufferSize, stream.ID, d.Logger,
		stream.Recv,
		func() { stream.heartbeatsRecvThresholdTicker.Reset(recvThreshold) },
		func(ctx context.Context, input *pb.HTTPProxyResponse, receivedAt time.Time) error {
			return d.processHTTPInput(ctx, stream, input, receivedAt)
		},
	)
}

// processHTTPInput handles a single message received from an HTTP-mode agent.
// Heartbeats (nil response or empty transaction ID) are forwarded to the heartbeat channel.
// Frames a session hook claims go to the session's part (SessionHook).
// Data messages are enriched with metadata and forwarded via the broker.
func (d *Dispatcher) processHTTPInput(ctx context.Context, stream *HTTPStream, input *pb.HTTPProxyResponse, receivedAt time.Time) error {
	if !stream.moving.Load() {
		_ = d.refreshAgentInventoryOnInboundActivity(ctx, stream.ID, d.IPInventory, stream.Session(), stream.Labels(), stream.Capabilities()) // superseded: the session's watch ends it
	}
	logger := logging.With(d.Logger, "AgentID", stream.ID)
	if input.Response == nil && input.GetMove() == pb.Move_MOVE_CONNECTED {
		stream.noteMoveConnected() // agent_move.go
	}
	if input.Response == nil || len(input.Response.TransactionId) == 0 {
		logger.Debug("received a heartbeat")
		if ctx.Err() != nil {
			return ctx.Err()
		}
		d.markAgentStreamAlive(stream, input)
		return nil
	}

	// A frame a session hook claims goes to the session's part, unread.
	if h := d.SessionHook; h != nil && h.Claims(input.Response.TransactionId) {
		if p := stream.hooked; p != nil {
			p.Frame(input.Response.TransactionId, input.Response.Payload)
		}
		return nil
	}

	input.Timestamps.ResponseAtGrpcServer = receivedAt.UnixMilli()
	stream.bytesRecv.Add(int64(len(input.Response.Payload)))
	// to be backward compatible with agents, set new fields to zero values
	if input.Routing == nil {
		input.Routing = &pb.RoutingInfo{}
	}
	// overrides agentID received from agent with the known stream agentID
	input.Routing.AgentId = stream.ID
	delete(input.Response.Headers, agentGoneHeader) // the relay's own marker
	logger = logging.With(d.Logger, "AgentID", input.Routing.AgentId, "transaction_id", input.Response.TransactionId)
	now := time.Now()
	if ctx.Err() != nil {
		// The session ended and its receive loop drains what the agent sent
		// before: deliver it on a bound of its own (the in-memory broker
		// drops a send on an ended ctx without an error).
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.WithoutCancel(ctx), agentGoneDeliveryBound)
		defer cancel()
	}
	txn, ends := input.Response.TransactionId, endsTransaction(input.Response) // the proxy owns input once published
	err := d.routeResponseToBroker(ctx, input)
	if err != nil {
		return fmt.Errorf("sending message for transactionID: %s: %w", txn, err)
	}
	if ends { // only once delivered: else failInflight answers it
		stream.inflight.Delete(txn)
	}
	logger.Debug("routeResponseToBroker - sending message to producer took", "duration", time.Since(now))
	logger.Debug("responding to transaction took", "transaction_id", txn, "duration", time.Since(now))
	metrics.IncrQueuedRequests()
	return nil
}

// routeResponseToBroker publishes an agent's HTTP response to this pod's
// topic, where the waiting HTTP proxy consumes it.
func (d *Dispatcher) routeResponseToBroker(ctx context.Context, input *pb.HTTPProxyResponse) error {
	publishStart := time.Now()
	defer func() {
		metrics.RegisterBrokerPublishDuration(float64(time.Since(publishStart).Milliseconds()))
	}()
	_, err := d.podIDTopicProducer.SendProto(ctx, input)
	return err
}
