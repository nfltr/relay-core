# A gRPC connection with no stream is closed (2026-11-11)

Lane RELAYPERF, from the audit of iroh-relay's open relay bugs (iroh issue
4630: no deadline on the identity handshake after the WebSocket upgrade).

## Gap

On the gRPC port a connection's identity arrives with a stream's headers
(the auth interceptor reads `x-api-key` or the certificate when a stream
starts). `grpc.connection_timeout` (10 s) bounds the TLS handshake and the
HTTP/2 preface; nothing bounded the time after that. A client that sends the
preface and settings, answers the server's keepalive pings (every HTTP/2
library does) and never opens a stream held its connection, with its TLS and
HTTP/2 buffers, for good. The per-IP limits (`security.conn_limit.*`) cap how
many one address holds, but a deployment that turns them off (one NAT
address for every client) had no bound at all.

## Change

`grpc.max_connection_idle` (default 5 min; 0 = never) sets grpc-go's
`keepalive.ServerParameters.MaxConnectionIdle`: a connection with no active
stream for that long gets GOAWAY and is closed after the GOAWAY ping (or
grpc-go's own short bound when the ping is not answered).

Invariants: a connection with an open stream is never closed by it (grpc-go
starts the idle timer only when the last stream ends); a client that was
idle reconnects at its next call (GOAWAY is graceful; grpc-go clients redial
on the next RPC). Every nfltr agent holds its `ConnectHTTP`/`ConnectTCP`
stream for its life, so agents are not affected; a client idle for 5 min
pays one TLS handshake at its next call.

Failure model: the peer stalls after the preface (closed at the bound), the
peer ignores GOAWAY (closed after the second GOAWAY), the peer stops
answering pings (the keepalive `Timeout` closes it first, as before).

TLA+: not applicable; a timeout on one connection's lifetime, no state
machine, ordering or protocol change.

Test: `grpcfactory/idle_conn_test.go`
`TestServerClosesAConnectionThatOpensNoStream` (preface, settings, pings
answered, no stream: closed within the bound; fails without the option).
