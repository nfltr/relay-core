<p align="center"><img src="docs/logo.svg" width="96" alt="relay-core"></p>

# relay-core

`github.com/nfltr/relay-core` is the session core of the nfltr relay as a Go
module: agents behind NAT dial out to it over gRPC and register; clients that
hold the same key open byte sessions to those agents' ports and services
through it, or send them HTTP requests. It authenticates every connection
through a chain of identity providers (a key file by default, your own
`identity.Provider` otherwise), and runs one replica with an in-process
directory: no database, no Redis, no hosted services.

It is a library first (`compose.Serve`, the `core/...` packages, frontend
plugins) and ships a small command, `nfltr-relay-core`, that runs it on its
own. The nfltr relay server (`nfltr-relay`) is built on it through its
exported API only.

Licensed Apache-2.0 ([LICENSE](LICENSE), [NOTICE](NOTICE)). Security reports:
[SECURITY.md](SECURITY.md). Contributions: [CONTRIBUTING.md](CONTRIBUTING.md).

## Quickstart

Go (the version `go.mod` names), git and a POSIX shell. This clones the
module, builds the relay and the quickstart program, starts the relay with
no configuration (loopback only; clients on 8080, agents on 9090; a key
written to `nfltr-relay-core.keys`, mode 0600), connects an agent in front of
a TCP echo server, and sends bytes from a client through the relay to it:

<!-- quickstart -->
```sh
git clone https://github.com/nfltr/relay-core && cd relay-core
go build -o bin/ ./cmd/nfltr-relay-core ./examples/quickstart
bin/nfltr-relay-core serve 2>relay.log &
trap "kill $!" EXIT
bin/quickstart -key-file nfltr-relay-core.keys -agents 127.0.0.1:9090 -clients 127.0.0.1:8080
```

It prints `echoed: hello through nfltr-relay-core`. `make readme-quickstart`
runs this block as written (on free ports) and `scripts/time-to-first-byte.sh
[source]` times it from `git clone` to the echoed bytes.

The agent and the client are the relay's client SDK,
`github.com/nfltr/relay/client/agent` and the data connection of
`github.com/nfltr/relay/proto/wire/dataplane`; see
[examples/quickstart](examples/quickstart/main.go).

### Beyond loopback

Off loopback the relay serves TLS only; it refuses to listen elsewhere in
plaintext and says what to set. Bring your own certificate and key file:

```sh
printf 'alice:%s\n' "rpc_$(openssl rand -hex 24)" >keys && chmod 600 keys
LISTEN_ADDRESS=0.0.0.0 TLS_ENABLE=true TLS_CERT_FILE=relay.crt TLS_KEY_FILE=relay.key \
SECURITY_APIKEY_FILE=keys TCP_ENABLED=true bin/nfltr-relay-core serve --from-env
```

Each key file line is `<agent id>:<key>` (or `<agent id>:sha256:<hex>`, the
key's hash); agents and clients send the key. `serve --from-env` reads every
setting from the environment (`PROXY_PORT`, `GRPC_PORT`, `INTERNAL_PORT`,
`METRICS_PORT`, `LISTEN_ADDRESS`, `SECURITY_APIKEY_FILE`, `NFLTR_API_KEY`,
`TCP_ENABLED`, `TLS_*`, ...; `platform/config` lists them, a key's
environment variable is its name upper-cased with `.` and `-` as `_`).
`/ready` and `/health` are on the internal port, Prometheus metrics on the
metrics port; on SIGTERM it drains (no new clients, open sessions run to the
grace period's end).

## As a library

- [examples/minimal](examples/minimal/main.go): a relay core, an agent and a
  client in one process (`compose.Serve` on your own listeners).
- [examples/custom-auth](examples/custom-auth/main.go): your own
  `identity.Provider` in place of the key file.
- [examples/custom-frontend](examples/custom-frontend/main.go): your own
  `frontend.Frontend` attached to the core: a plain TCP port that carries
  each connection to one agent's service.
- `relaycoretest.New(t)` starts a relay core on loopback in a test, as
  `httptest.NewServer` starts an HTTP server.

`make examples` builds and runs each of them; `go doc` documents every
exported package and symbol.

## Dependencies

The standard library, `github.com/nfltr/link` (with its QUIC
`peerlink/quiclink`) and the relay's protocol and client modules
`github.com/nfltr/relay/proto` and `github.com/nfltr/relay/client`. Linked
today besides (`cmd/nfltr-relay-core/links_test.go` fails on any other):

- gRPC and protobuf (`google.golang.org/grpc`, `google.golang.org/protobuf`,
  `google.golang.org/genproto`, `github.com/grpc-ecosystem/...`);
- Prometheus (`github.com/prometheus/client_golang` and its dependencies);
- viper and pflag (`github.com/spf13/...` and their dependencies), the
  settings;
- fsnotify (the key file's reload), golang-lru (`hashicorp/golang-lru/v2`);
- quic-go, as `github.com/nfltr/quic-go` (a fork), through quiclink;
- `github.com/flynn/noise` (the relay client's end-to-end encryption);
- `golang.org/x/...` (crypto, net, sync, sys, text).

No cgo. Every module it links is public.

## Limits

- No ACME and no automatic certificates: TLS is the operator's certificate
  and key (`tls.cert_file`, `tls.key_file`).
- No OIDC or other hosted identity: the key file, or an `identity.Provider`
  you write.
- One replica: the directory is in-process; there is no replica link
  between relays in `compose.Serve`.
- The QUIC agent link is optional and off: `compose.Serve` and
  `nfltr-relay-core` serve agents over TCP; a composition that sets
  `compose.AgentPort.QUIC` (and TLS) serves it beside.
- Configuration is process-wide (viper), so one process runs one
  configuration; `relaycoretest` relays in one process share it.

## Versioning

Semantic versioning, v0.x: a minor version (v0.N.0) may change the exported
API, each change listed in [CHANGELOG.md](CHANGELOG.md) with its migration; a
patch version does not change it. `make check-api` compares the API against
the latest release tag.

## Development

`make ci` runs what the workflow (`.github/workflows/check.yml`) runs: build,
vet, tests with `-race`, short fuzzing, gosec, semgrep (docker), govulncheck,
the publish check, the binary size budget, the examples and this README's
quickstart. `make hooks` installs a pre-push hook that runs it.
