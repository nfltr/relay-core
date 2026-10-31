// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package config registers the relay's settings as flags (SetDefaultFlags)
// read through viper: a set flag, then the environment (a key upper-cased,
// "." and "-" as "_"), then the default. ConfigWatcher reloads the settings
// that change at runtime.
package config

import (
	"fmt"
	"strings"
	"time"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// ─── Exported constants ─────────────────────────────────────────────────────

// Config keys of the relay's ports, and the values some settings take
// (cache.type, the Redis deployment, env).
const (
	// Ports
	GrpcServerPort  = "grpc-port"
	ProxyServerPort = "proxy-port"
	// SNIRouterPort is the E2EE passthrough SNI router's own port (the
	// relay applies no TLS on the gRPC port, so no SNI mux there).
	SNIRouterPort = "e2ee.tls_passthrough.port"

	// Cache types
	InMemoryCache = "inmemory"
	RedisCache    = "redis"
	NoCache       = "nocache"

	// Redis deployment types
	RedisStandalone  = "standalone"
	RedisReplication = "replication"

	// Git operations backends
	GitOpsExec = "exec"

	// Environment names
	EnvLocal      = "local"
	EnvStandalone = "standalone"
	EnvCI         = "CI"
	EnvK8s        = "k8s"

	// gRPC max message size default (8 MiB)
	DefaultGRPCMaxMessageSize = 8 << 20
)

// IsLocalEnv reports whether env belongs to the local-style configuration
// group used for developer and single-node runtime validation rules.
func IsLocalEnv(env string) bool {
	normalized := strings.ToLower(strings.TrimSpace(env))
	return normalized == EnvLocal || normalized == EnvStandalone
}

// IsK8sMultiPodEnv reports whether env should enable k8s-style multi-pod behavior.
// CI is included because repo integration/smoke suites run multi-pod flows under CI.
func IsK8sMultiPodEnv(env string) bool {
	normalized := strings.ToLower(strings.TrimSpace(env))
	return normalized == EnvK8s || normalized == strings.ToLower(EnvCI)
}

// GRPCMaxMessageSizeBytes returns the configured max gRPC message size in bytes.
// Falls back to DefaultGRPCMaxMessageSize when unset or invalid.
func GRPCMaxMessageSizeBytes() int {
	v := viper.GetInt("grpc.max_message_size")
	if v <= 0 {
		return DefaultGRPCMaxMessageSize
	}
	return v
}

// ─── Default values (private, grouped by subsystem) ─────────────────────────

const (
	// Logging
	defaultLogLevel = "info"

	// Ports
	defaultGrpcServerPort  = "9090"
	defaultProxyServerPort = "8080"

	// Request timeouts
	defaultRequestTimeout = 60 * time.Second

	// Termination
	defaultTerminationGracePeriodSeconds = 30

	// Health / Internal
	defaultInternalEnable    = true
	defaultInternalAddress   = "0.0.0.0"
	defaultInternalPort      = "8081"
	defaultInternalHealth    = "/healthz"
	defaultInternalReadiness = "/ready"

	// Metrics & Profiling
	defaultMetricsEnable   = true
	defaultMetricsPort     = "8082"
	defaultProfilingEnable = false

	// Behavior flags
	defaultOnDownstreamErrorsDropAgentConn = false
)

// SetDefaultFlags registers all configuration keys with pflag + viper.
// Flags are grouped by subsystem; each group is separated by a blank line.
func SetDefaultFlags() {
	// ── Environment & Identity ──────────────────────────────────────────
	_ = flag.String("env", "", "deployment environment (e.g. local, CI)")
	_ = flag.String("loopback", "127.0.0.1", "loopback IP used for local connections")
	_ = flag.String("pod_id.env_var", "MY_POD_NAME", "env var holding the Kubernetes pod name")
	_ = flag.String("pod_ip.env_var", "MY_POD_IP", "env var holding the Kubernetes pod IP")

	// ── Ports & Networking ──────────────────────────────────────────────
	_ = flag.String(GrpcServerPort, defaultGrpcServerPort, "gRPC listen port for agent connections")
	_ = flag.String(ProxyServerPort, defaultProxyServerPort, "HTTP listen port for REST proxy requests")
	_ = flag.String("listen.address", "", "bind address of the proxy and gRPC ports, or a comma-separated list of addresses all bound on the same ports (empty: all interfaces)")

	// ── Dispatcher ──────────────────────────────────────────────────────
	_ = flag.Bool("rpc.server.enable", true, "start the gRPC server for agent connections")
	_ = flag.Bool("http.server.enable", true, "start the HTTP server for REST proxy")

	// ── Request Timeouts ────────────────────────────────────────────────
	_ = flag.Duration("default.request.timeout", defaultRequestTimeout, "max wait for an agent's response to start or send its next chunk (not a cap on the whole response)")

	// ── Termination ─────────────────────────────────────────────────────
	_ = flag.Int("termination.grace.periodseconds", defaultTerminationGracePeriodSeconds, "seconds from SIGTERM to SIGKILL (a pod's terminationGracePeriodSeconds less its preStop hook's); the drain ends 5 s before")

	// ── Logging ─────────────────────────────────────────────────────────
	_ = flag.String("logging.level", defaultLogLevel, "log verbosity: debug, info, warn, error")

	// ── Runtime Config ──────────────────────────────────────────────────
	_ = flag.String("runtime.config.path", "", "path to a YAML file (typically ConfigMap-mounted) watched for runtime config changes")

	// ── UI Assets ───────────────────────────────────────────────────────
	_ = flag.String("ui.dev_dir", "", "serve UI files from this directory instead of embedded assets")

	// ── Health / Internal ───────────────────────────────────────────────
	_ = flag.Bool("internal.enable", defaultInternalEnable, "start the internal HTTP server (health, readiness)")
	_ = flag.String("internal.address", defaultInternalAddress, "bind address for the internal HTTP server and the metrics server")
	_ = flag.String("internal.port", defaultInternalPort, "listen port for the internal HTTP server")
	_ = flag.String("internal.health", defaultInternalHealth, "URL path for liveness health checks")
	_ = flag.String("internal.readiness", defaultInternalReadiness, "URL path for readiness probes")

	// ── Metrics & Profiling ─────────────────────────────────────────────
	_ = flag.Bool("metrics.enable", defaultMetricsEnable, "expose Prometheus metrics endpoint")
	_ = flag.String("metrics.port", defaultMetricsPort, "listen port for the Prometheus metrics server")
	_ = flag.Bool("profiling.enable", defaultProfilingEnable, "expose /debug/pprof profiling endpoints")

	// ── Heartbeat ───────────────────────────────────────────────────────
	_ = flag.Bool("rpc.agent.heartbeat.enable", true, "enable periodic heartbeat pings to connected agents")
	// The relay states its heartbeat in each agent stream's accept headers;
	// an agent ends a connection silent for three of them, so 5s (was 15s)
	// notices a relay that died without a FIN reaching the agent within 15s.
	// It also
	// keeps consumer-NAT mappings warm. Last-seen and WireGuard lease
	// refreshes stay at most every 15s. The recv threshold (agent→relay) is
	// unchanged at 45s.
	_ = flag.Duration("rpc.agent.heartbeat.sendduration", 5*time.Second, "interval between heartbeat pings sent to each agent (server→agent); agents end a connection silent for three of them")
	_ = flag.Duration("rpc.agent.heartbeat.recvthreshold", 45*time.Second, "max time without a heartbeat from agent before disconnect")

	// ── Git operations ────────────────────────────────────────────────────
	_ = flag.String("gitops.type", GitOpsExec, "git operations backend: exec | ...")

	// ── Cache & Redis ───────────────────────────────────────────────────
	_ = flag.String("cache.type", RedisCache, "response cache backend: redis, inmemory, or nocache")
	_ = flag.String("cache.redis_replication", RedisReplication, "Redis deployment mode: standalone or replication (sentinel)")
	_ = flag.String("cache.redis_url", "localhost:6379", "Redis address (host:port) for standalone mode")
	_ = flag.String("cache.redis_master_name", "rpc-server-redismaster", "Sentinel master name for replication mode")
	_ = flag.StringSlice("cache.redis_sentinel_addrs", []string{"localhost:26379"}, "Sentinel node addresses for replication mode")
	_ = flag.Int("cache.connection_retry_count", 3, "max connection attempts to Redis before giving up")
	_ = flag.Duration("redis.ops_timeout", 3*time.Second, "max time for a single directory operation: a Redis GET/SET, a statement on the SQL agents table")
	_ = flag.Int("redis.pool_size", 10, "max connections in the Redis connection pool")
	_ = flag.Int("redis.min_idle_conns", 2, "idle connections kept warm in the Redis pool")
	_ = flag.String("redis.endpoint", "", "external Redis hostname (overrides cache.redis_url)")
	_ = flag.String("redis.port", "6379", "external Redis port (used with redis.endpoint)")

	// ── Blob store (nfltr.blob.v1: /api/v1/blobs) ────────────────────────
	// The blob service holds the blobs: the
	// relay runs it as its own child process with these blob_store.*
	// settings (blobs.command), or (blobs.service) carries the routes to
	// service agents that run apart.
	_ = flag.String("blob_store.type", "", "blob store backend: filesystem (single relay) or s3 (replicas sharing a bucket); the relay runs the blob service as its own child process on it; unset: no child (see blobs.service)")
	_ = flag.String("blob_store.dir", "", "filesystem blob store directory (blob_store.type=filesystem)")
	_ = flag.String("blob_store.s3.endpoint", "", "S3-compatible endpoint host[:port] (blob_store.type=s3)")
	_ = flag.String("blob_store.s3.bucket", "", "S3 bucket holding blobs; created when absent")
	_ = flag.String("blob_store.s3.region", "", "S3 region (optional)")
	_ = flag.String("blob_store.s3.access_key", "", "S3 access key; set via BLOB_STORE_S3_ACCESS_KEY; empty uses the AWS/MinIO env and instance-metadata chain")
	_ = flag.String("blob_store.s3.secret_key", "", "S3 secret key; set via BLOB_STORE_S3_SECRET_KEY, never on the command line")
	_ = flag.Bool("blob_store.s3.insecure", false, "use plain HTTP to the S3 endpoint (in-cluster MinIO only)")
	_ = flag.Int64("blob_store.max_object_bytes", 0, "largest blob an upload may declare; 0 keeps the default (1 GiB). Uploads travel in parts of at most 64 MiB regardless")
	_ = flag.Duration("blob_store.retention", 7*24*time.Hour, "how long a blob is kept after its owner last put it, and the longest read grant; must be positive")
	_ = flag.String("blobs.service", "", "the relay: blob service agents the /api/v1/blobs routes go to, <owner>/<agent>[,<owner>/<agent>...] (each declares the tcp service blobs); excludes blob_store.type")
	_ = flag.String("blobs.command", "nfltr-blobs", "the blob service binary the relay runs as its child (blob_store.type): a path, or a name looked up next to this binary, then on PATH")

	// ── TLS ─────────────────────────────────────────────────────────────
	_ = flag.Bool("tls.enable", false, "enable TLS on all external listeners (gRPC, HTTP proxy)")
	_ = flag.String("tls.cert_file", "", "path to PEM-encoded TLS certificate")
	_ = flag.String("tls.key_file", "", "path to PEM-encoded TLS private key")
	_ = flag.Bool("tls.auto", false, "auto-generate a self-signed certificate when cert/key are not provided (development only)")
	_ = flag.Bool("tls.grpc_only", false, "apply TLS only to the gRPC listener, leave HTTP proxy plaintext (useful when TLS is terminated by a reverse proxy for HTTP)")
	_ = flag.String("tls.san_dns", "", "comma-separated extra DNS SANs to include in auto-generated self-signed certificate (e.g. grpc.example.com,*.example.com)")
	_ = flag.Bool("tls.acme", false, "obtain TLS certificates from Let\u2019s Encrypt via ACME TLS-ALPN-01 (requires port 443 reachable from the internet)")
	_ = flag.String("tls.acme_domains", "", "comma-separated domain whitelist for ACME certificates (e.g. grpc.example.com)")
	_ = flag.String("tls.acme_cache_dir", "/var/cache/autocert", "directory to cache ACME certificates")
	_ = flag.String("tls.acme_email", "", "contact email for Let\u2019s Encrypt notifications (optional)")
	_ = flag.String("tls.web_domains", "", "comma-separated hostnames whose TLS connections on the gRPC port are served by the HTTP handler (direct-origin web, no CDN); added to the ACME whitelist. A \"*.<domain>\" entry serves every single-label subdomain except the ACME names, with a Let\u2019s Encrypt wildcard certificate obtained over DNS-01 (needs tls.acme and tls.acme_dns01_cloudflare_token_file)")
	_ = flag.String("tls.acme_dns01_cloudflare_token_file", "", "file holding a Cloudflare API token with Zone DNS Edit on the zone of each \"*.<domain>\" in tls.web_domains, for the ACME DNS-01 challenge")
	_ = flag.String("http.redirect_port", "", "when set with tls.web_domains, a plain HTTP port (e.g. 80) that answers ACME HTTP-01 challenges and redirects everything else to https")

	// ── API Key Authentication ──────────────────────────────────────────
	_ = flag.Bool("security.apikey.enable", false, "require API key for agent connections")
	_ = flag.String("security.apikey.file", "/etc/rpcserver/api-keys", "path to file containing valid API keys (one per line, format: label:key)")
	_ = flag.String("security.apikey.env", "NFLTR_API_KEY", "environment variable containing a single API key (fallback if file is missing)")

	// ── mTLS identity provider (client certificates; off without a CA file) ──
	_ = flag.String("security.mtls.ca_file", "", "PEM bundle of the CAs whose client certificates authenticate (one or more; reloaded on change); turns the mTLS identity provider on (needs tls.enable)")
	_ = flag.String("security.mtls.revoked_file", "", "optional revocation list, reloaded on change: a CRL (PEM or DER) signed by a CA of the bundle, or hex serials one per line; a revoked certificate's connections end")
	_ = flag.String("security.mtls.owner", "{cn}", "rule making a certificate's owner (namespaced mtls:<owner>) from its fields {cn} {o} {ou} {dns} {email} {uri} {serial}")
	_ = flag.String("security.mtls.subject", "{cn}", "rule making a certificate's subject (its agent id) from the same fields")
	_ = flag.StringSlice("security.mtls.names", nil, "agent names a certificate may take under its subject (\"*\" any, \"ci-*\" a prefix; empty: any)")

	// ── OIDC/JWT identity provider (an organization's IdP; off without an issuer) ──
	_ = flag.String("security.oidc.issuer", "", "issuer URL whose JWTs authenticate (the iss claim; https, or http to loopback); turns the OIDC identity provider on")
	_ = flag.String("security.oidc.audience", "", "value the tokens' aud claim must hold (required with an issuer)")
	_ = flag.String("security.oidc.jwks_url", "", "the issuer's JWKS URL (empty: from <issuer>/.well-known/openid-configuration)")
	_ = flag.String("security.oidc.owner", "{sub}", "rule making a token's owner (namespaced oidc:<issuer hash>:<owner>) from its claims, e.g. {org}")
	_ = flag.String("security.oidc.subject", "{sub}", "rule making a token's subject (its agent id) from its claims")
	_ = flag.String("security.oidc.scopes_claim", "scope", "claim holding a token's scopes (space-separated or a list; admin is never taken from an IdP)")
	_ = flag.String("security.oidc.names_claim", "", "claim holding the agent names a token may take (empty: any name)")
	_ = flag.Duration("security.oidc.clock_skew", time.Minute, "clock skew allowed on exp, nbf and iat (at most 5m); a connection ends at exp plus this")

	// ── SQL Key Store (database-backed API keys & fleet tokens) ─────────
	_ = flag.Bool("security.sql.enable", false, "enable SQL-backed API key and fleet token store")
	_ = flag.String("security.sql.dsn", "/var/lib/rpcserver/keys.db", "database path (SQLite) or connection string (Postgres)")
	_ = flag.String("security.sql.dialect", "sqlite", "SQL dialect: sqlite or postgres")
	_ = flag.Bool("security.sql.migrate_on_startup", true, "run SQL schema migrations when the server starts")
	_ = flag.Int("security.sql.max_conns", 16, "Postgres connections per pool: the open and idle cap of each pool the relay opens (the key store, and the accounts read store of accounts.read_dsn: at most twice this per replica), so busy connections are reused instead of reopened (SQLite keeps its own pools)")
	_ = flag.Duration("security.sql.conn_max_idle_time", 5*time.Minute, "close a pooled Postgres connection idle this long (0 = never)")
	_ = flag.Bool("security.sql.import_file_keys", true, "import existing file-based API keys into the SQL store on startup")

	// ── IP Allowlist ────────────────────────────────────────────────────
	_ = flag.StringSlice("security.ip_allowlist", nil, "CIDR ranges allowed to connect (empty = allow all)")

	// ── Audit Logging ───────────────────────────────────────────────────
	_ = flag.Bool("security.audit.enable", false, "enable structured audit logging for security events")

	// ── Browser Sessions ────────────────────────────────────────────────
	_ = flag.Bool("security.session.enable", false, "enable cookie-based browser sessions (requires API key auth)")
	_ = flag.String("security.session.secret_env", "RPC_SESSION_SECRET", "environment variable containing the HMAC-SHA256 session signing secret")
	_ = flag.Duration("security.session.ttl", 24*time.Hour, "session cookie TTL")
	_ = flag.Bool("security.session.secure", true, "set Secure flag on session cookie (false for HTTP-only dev environments)")

	// ── Google OAuth ────────────────────────────────────────────────────
	_ = flag.Bool("security.google.enable", false, "enable Google OAuth2 login for the admin dashboard")
	_ = flag.String("security.google.client_id_env", "GOOGLE_CLIENT_ID", "environment variable containing the Google OAuth client ID")
	_ = flag.String("security.google.client_secret_env", "GOOGLE_CLIENT_SECRET", "environment variable containing the Google OAuth client secret")
	_ = flag.String("security.google.redirect_url", "", "full OAuth callback URL (e.g., https://rpc.example.com/oauth/google/callback)")
	_ = flag.StringSlice("security.google.allowed_emails", nil, "email addresses allowed to log in via Google OAuth")
	_ = flag.StringSlice("security.google.allowed_domains", nil, "email domains allowed to log in via Google OAuth (e.g., example.com)")
	_ = flag.String("security.google.auth_url", "", "override the Google OAuth authorize endpoint (for mock testing)")
	_ = flag.String("security.google.token_url", "", "override the Google OAuth token endpoint (for mock testing)")
	_ = flag.String("security.google.userinfo_url", "", "override the Google OAuth userinfo endpoint (for mock testing)")
	_ = flag.StringSlice("security.admin_emails", nil, "email addresses allowed to manage global settings (empty = all authenticated users)")
	_ = flag.Bool("usage.page_visits", false, "count views of the public pages (landing, docs, blog, install) per day for the admin usage report: counts only, no cookies or scripts, no IP or User-Agent stored; needs the SQL user store; env USAGE_PAGE_VISITS")
	_ = flag.String("helper.url", "", "loopback base URL of the nfltr.xyz docs helper process (nfltr-site-helper), e.g. http://127.0.0.1:8095; set, the relay serves /help, the docs Ask box and proxies POST /api/help/ask to it; empty (default) = off; env HELPER_URL")
	_ = flag.String("security.users.import_file", "/etc/rpcserver/users.json", "legacy JSON users file imported once into the SQL user store at startup, only while the users table is empty (missing file = nothing to import)")
	_ = flag.Bool("security.users.in_memory", false, "DEV/TEST ONLY: with Google OAuth and no security.sql.enable, keep dashboard users in a throwaway in-memory SQLite database (sign-ins and agent ownership are lost on restart); without it such a relay refuses to start")

	_ = flag.String("agent_link.quic.listen", "", "UDP address (e.g. :443) where agents' agent-link connections arrive over QUIC, with the gRPC port's TLS certificate; agents use it only when they ask for it (nfltr --transport quic|auto); empty (default) = TCP only; needs TLS; env AGENT_LINK_QUIC_LISTEN")
	_ = flag.Int("security.min_client_generation", 0, "refuse clients below this generation at connect (gRPC calls and tcp-connect): 2 refuses every nfltr before v2.0.0 with a message to run `nfltr update`; 0 (default) takes every client and counts and logs those below generation 2 (rpc_server_dispatcher_old_generation_clients_total); env SECURITY_MIN_CLIENT_GENERATION")

	// ── Authorization ───────────────────────────────────────────────────
	_ = flag.Bool("security.trust_loopback", false, "DEV ONLY: trust requests from a loopback address (127.0.0.1/::1) as a fixed local principal with no credential, so `nfltr orch` runs locally with zero auth setup. Loopback-only; never enable in production. Env: SECURITY_TRUST_LOOPBACK=true.")

	// ── API Type Detection ────────────────────────────────────────────
	_ = flag.StringSlice("http.api_type_prefixes", []string{"wapi", "ibap"}, "URL path prefixes recognized as distinct API types for metrics labeling")

	// ── HTTP Server ─────────────────────────────────────────────────────
	_ = flag.Duration("http.server.read_header_timeout", 10*time.Second, "max time to read request headers before timeout")
	_ = flag.Duration("http.server.idle_timeout", 120*time.Second, "close keep-alive connections idle longer than this")
	_ = flag.Duration("http.server.write_timeout", 60*time.Second, "max time one response write may stall; each write extends it, so a response that keeps streaming is never cut")
	_ = flag.Int("http.server.producers.cache_size", 0, "max agentID-to-producer mappings cached in HTTP server (0 = no limit: an agent's producer is dropped when it disconnects)")
	_ = flag.Duration("http.server.producers.cache_expiry", 1*time.Hour, "TTL for cached agentID-to-producer mappings")
	_ = flag.Int("http.server.fanout_workers", 1, "goroutines dispatching broker responses to waiting HTTP handlers; chunk reordering ensures correctness at any value")
	_ = flag.Int64("http.server.max_request_body_size", 1<<30, "maximum request body size in bytes (default 1 GiB); larger bodies get a 413. Bodies stream to agents that advertise upload flow control; agents that read bodies whole stay capped at 10 MiB")
	_ = flag.Int("http.server.uploads.per_ip", 4, "request bodies over 1 MiB (or of unknown length) in flight, streaming or waiting, per client IP; more get a 429 with Retry-After (0: no limit)")
	_ = flag.Int64("http.server.uploads.buffer_bytes", 128<<20, "upload bytes the relay may hold in flight across all clients; each body over 1 MiB reserves min(its length, 8 x grpc.max_message_size) while it streams, others wait (0: no bound)")
	_ = flag.Duration("http.server.uploads.wait", 30*time.Second, "how long an upload waits for http.server.uploads.buffer_bytes space before a 503 with Retry-After; keep it under http.server.write_timeout")

	// ── Share URLs ──────────────────────────────────────────────────────
	_ = flag.String("http.share_domain", "", "wildcard domain for subdomain share URLs (e.g. rpc.example.com); requires a *.rpc.example.com DNS record pointing to this server")
	_ = flag.Duration("http.share_ttl", 24*time.Hour, "lifetime of a share code; after this the subdomain/path stops working")
	_ = flag.String("http.share_store_file", "", "SQLite file holding issued share codes (a relay that keeps them: nfltr-relay), mailboxes and webhooks when there is no SQL key store, so they survive a restart; empty: in memory")
	_ = flag.Duration("http.share_reservation_ttl", 2*time.Hour, "how long to reserve a used share code for the same agent before it may be reassigned; 0 disables reservation")
	// Gap 7: direct agent subdomain routing — <agentID>.share_domain reaches the
	// live agent for its owner (API key); anyone else needs a share code.
	_ = flag.Bool("http.direct_agent_subdomains", false, "route <agentID>.<share_domain> to the matching agent for its owner (API key); visitors use share codes")
	_ = flag.String("mailbox.service", "", "the relay: mailbox service agents (cmd/nfltr-mailbox) the mailbox, webhook and web push routes go to, <owner>/<agent>[,<owner>/<agent>...] (each declares the tcp service mailbox; a key served from the API key file, not imported into the SQL store, is owner keys:<id>); empty: this process serves them when it links the service (nfltr-relay), else it refuses to start (cmd/server); env MAILBOX_SERVICE")
	_ = flag.String("accounts.service", "", "the relay: accounts service agents (cmd/nfltr-accounts) the dashboard host (dashboard.<http.share_domain>) goes to, <owner>/<agent>[,...] (each declares the tcp service accounts); needs accounts.read_dsn; empty: this process serves sign-in and the dashboard when it links the service; env ACCOUNTS_SERVICE")
	_ = flag.String("accounts.read_dsn", "", "the accounts service's database, opened read-only (key, session and owner lookups; the revocation journal), dialect security.sql.dialect; env ACCOUNTS_READ_DSN")
	_ = flag.StringSlice("http.site_hosts", nil, "hosts under http.share_domain served by one account's agent: <label>=<owner>/<agent>[/<service>], any DNS label (a product name such as orch, or another); a mapped label is reserved on this relay like the product names: no agent, share or direct subdomain takes it. e.g. orch=ops@example.com/site-key.site/orch; env HTTP_SITE_HOSTS")
	_ = flag.String("http.site_hosts_file", "", "a file holding the http.site_hosts entries instead (comma-, space- or newline-separated, # comments), read again whenever it changes: a site host is added or removed without a restart; a file that does not parse is refused whole and the table before it stays; not with http.site_hosts; env HTTP_SITE_HOSTS_FILE")
	_ = flag.String("http.site_apex", "", "the agent serving http.share_domain itself and www: <owner>/<agent>[/<service>]; the relay keeps its own paths and every request with a relay credential; env HTTP_SITE_APEX")
	_ = flag.Duration("endpoint.registry.offline_prune_ttl", 0, "delete persisted endpoint-registry entries whose last_seen_at is older than this duration; 0 disables pruning")

	// ── HTTP Reverse Proxy ──────────────────────────────────────────────
	_ = flag.Duration("proxy.dial_timeout", 5*time.Second, "max time to dial a dispatcher pod via reverse proxy")
	_ = flag.Duration("proxy.response_header_timeout", 60*time.Second, "max wait for first response header from the dispatcher")
	_ = flag.Duration("proxy.idle_conn_timeout", 90*time.Second, "close reverse proxy connections idle longer than this")
	_ = flag.Int("proxy.max_idle_conns_per_host", 1024, "idle connections retained per dispatcher host; keep it above the requests in flight, or every request above it opens and closes a connection and TIME_WAIT can exhaust the host's ports (G-L1)")
	_ = flag.Duration("proxy.tcp_keepalive", 30*time.Second, "TCP keepalive interval for reverse proxy connections")

	// ── RPC Server ──────────────────────────────────────────────────────
	_ = flag.Int("rpc.recv_buffer_size", 1000, "buffered channel capacity for inbound agent messages")
	_ = flag.Int("rpc.connected_agents_threshold", 0, "refuse new agents while this many are connected (0 = no limit); a refused agent stops with an error naming the limit")

	// ── Between replicas ────────────────────────────────────────────────
	// The replica link (link v2 over mTLS on its own port), the only path
	// between replicas; 0 = off: a multi-replica relay then serves only its
	// own agents; -1 = a port the kernel chooses, which peers read from this
	// replica's directory record.
	_ = flag.Int("peer_link.port", 0, "replica link port (link v2, mTLS), the only path between replicas; 0: off (agents on other replicas unreachable); -1: a port the kernel chooses, which peers read from this replica's directory record")
	_ = flag.String("peer_link.transport", "mtls", "replica link transport: mtls (the CA below), or plaintext where the pod network already encrypts or is trusted (needs S2S_TOKEN); one per release")
	_ = flag.String("peer_link.ca_cert_file", "", "replica link CA bundle (PEM; the first certificate signs this replica's certificate)")
	_ = flag.String("peer_link.ca_key_file", "", "replica link CA key (PEM)")
	_ = flag.String("peer_link.trust_domain", "nfltr-relay", "replica link trust domain: peers' certificates must name spiffe://<this>/replica/<pod>")
	_ = flag.String("peer_link.liveness", "default", "replica link liveness preset (default, fast, nat, datacenter, mobile) or interval=..,timeout=..")
	_ = flag.Duration("grpc.proxying.downstream.sync_map.clear_timeout", 5*time.Second, "grace period to drain pending requests before closing downstream")

	// ── gRPC Keepalive (server) ─────────────────────────────────────────
	_ = flag.Int("grpc.max_message_size", DefaultGRPCMaxMessageSize, "maximum gRPC message size in bytes for send/recv (applies to server and client call options)")
	// Defaults sized for long-lived idle worker streams behind Cloudflare. The
	// rpcclient default sends pings every 60s; min_time=30s keeps that well
	// under the server's TOO_MANY_PINGS enforcement. Server-initiated pings
	// every 30s detect dead client streams within ~40s and keep middleboxes
	// (Cloudflare, NAT) from idling the underlying TCP connection out.
	_ = flag.Duration("grpc.keepalive.enforcement_policy.min_time", 30*time.Second, "minimum allowed interval between client keepalive pings")
	_ = flag.Bool("grpc.keepalive.enforcement_policy.permit_without_stream", true, "allow client keepalive pings even when no RPC stream is active")
	_ = flag.Duration("grpc.keepalive.server_parameters.time", 30*time.Second, "interval between server-initiated keepalive pings to clients")
	_ = flag.Duration("grpc.keepalive.server_parameters.timeout", 10*time.Second, "time to wait for a keepalive ping ack from client")
	_ = flag.Uint32("grpc.max_concurrent_streams", 250, "max concurrent gRPC streams per connection (0 = unlimited)")

	// ── Behavior Flags ──────────────────────────────────────────────────
	_ = flag.Bool("on_downstream_errors.drop_agent_connection", defaultOnDownstreamErrorsDropAgentConn, "disconnect the agent when a downstream gRPC call fails")

	// ── Rate Limiting ───────────────────────────────────────────────────
	_ = flag.Bool("ratelimit.enable", true, "enable per-AgentID request rate limiting")
	_ = flag.Float64("ratelimit.rate", 100, "tokens added per second per AgentID")
	_ = flag.Int("ratelimit.burst", 200, "max token capacity (burst size) per AgentID")

	// ── Admin API ───────────────────────────────────────────────────────
	_ = flag.String("admin.message", "", "admin message displayed to agents on connect and on the landing page (empty = no message)")

	// ── Header & URL Rewriting (Gap 10) ─────────────────────────────────
	_ = flag.StringToString("http.request_headers.add", nil, "headers to inject into every proxied request (name=value pairs)")
	_ = flag.StringSlice("http.request_headers.remove", nil, "request header names to strip before forwarding to the agent")
	_ = flag.StringToString("http.response_headers.add", nil, "headers to inject into every response from the agent (name=value pairs)")
	_ = flag.StringSlice("http.response_headers.remove", nil, "response header names to strip before returning to the client")
	_ = flag.String("http.url_rewrite.pattern", "", "Go regex applied to request URL path before dispatch (empty = disabled)")
	_ = flag.String("http.url_rewrite.replacement", "", "replacement string for url_rewrite.pattern (supports $1 back-references)")

	// ── Fleet (IoT Command & Control) ───────────────────────────────────

	// ── TCP Tunneling (Gap 3 — SSH / RDP / Database) ───────────────────
	// tcp.enabled activates the raw TCP tunneling engine.  When false
	// (default) ConnectTCP returns Unimplemented.
	_ = flag.Bool("tcp.enabled", false, "enable raw TCP tunneling over gRPC (SSH, RDP, database, etc.)")
	// tcp.endpoints maps listen addresses to agentID|targetAddr pairs.
	// Example config file:
	//   [tcp.endpoints]
	//     ":2222" = "mydevice|localhost:22"        # SSH
	//     ":3389" = "mywinpc|localhost:3389"       # RDP
	//     ":5432" = "lan-gw|192.168.1.10:5432"    # LAN Postgres
	_ = flag.StringToString("tcp.endpoints", nil,
		"TCP tunnel endpoints: listenAddr=agentID|targetAddr (e.g. ':2222=mydevice|localhost:22')")

	// ── Native SSH/SCP Proxy ──────────────────────────────────────────
	// ssh.proxy.enabled exposes GET /connect/{agentID}/{port} which hijacks
	// the HTTP connection to a raw TCP pipe.  Users can then use native
	// ssh/scp/sftp via ProxyCommand="curl -sN http://server/connect/%h/22".
	// Requires tcp.enabled=true.  Defaults to false.
	_ = flag.Bool("ssh.proxy.enabled", false,
		"enable native SSH/SCP proxy via HTTP connection hijacking (requires tcp.enabled=true)")
	// ssh.proxy.target_host overrides the host portion of the target address
	// the agent dials.  The port comes from the URL path.  Default: localhost.
	_ = flag.String("ssh.proxy.target_host", "localhost",
		"target host the agent dials for SSH proxy connections (default: localhost)")

	// ── TCP / SSH Proxy Dial Retries ──────────────────────────────────
	// tcp.dial_max_retries controls how many times DialAgentWithRetry
	// attempts to reach an agent before returning an error.  Used by the
	// SSH proxy and browser terminal handlers.
	_ = flag.Int("tcp.dial_max_retries", 3,
		"max attempts to dial an agent for SSH proxy / terminal sessions")
	_ = flag.Duration("tcp.dial_retry_interval", 500*time.Millisecond,
		"base backoff interval between TCP dial retries (doubles each attempt, capped at 30s)")

	// ── End-to-end encryption of TCP sessions ────────────────────────
	// tcp.e2ee_policy is the operator's rule for the owner's TCP sessions
	// to named services (/connect): require refuses plain sessions and
	// agents that declare a plain service, forbid refuses E2EE sessions
	// and agents that declare an E2EE service, allow takes both.
	// tcp.e2ee_default is what a service that states nothing gets under
	// allow. A conflict is refused, never downgraded.
	_ = flag.String("tcp.e2ee_policy", E2EEAllow,
		"end-to-end encryption of TCP sessions to services: require, allow or forbid; env TCP_E2EE_POLICY")
	_ = flag.Bool("tcp.e2ee_default", false,
		"under tcp.e2ee_policy=allow, end-to-end encrypt the TCP services whose agent states no e2ee; env TCP_E2EE_DEFAULT")

	// ── E2EE TLS Passthrough ─────────────────────────────────────────
	// e2ee.tls_passthrough.enabled activates the SNI-based TLS passthrough
	// router.  When enabled, the server listens on a dedicated port, peeks
	// at the TLS ClientHello to extract the SNI hostname, maps it to an
	// agent ID, and forwards the raw encrypted bytes to the agent via the
	// TCP tunneling subsystem.  The server never terminates TLS for this
	// traffic.  Requires tcp.enabled=true.
	_ = flag.Bool("e2ee.tls_passthrough.enabled", false,
		"enable SNI-based TLS passthrough routing for E2EE connections")
	// e2ee.tls_passthrough.port is the TCP port for the SNI router listener.
	_ = flag.String("e2ee.tls_passthrough.port", "4443",
		"TCP port for E2EE TLS passthrough SNI router (default: 4443)")
	// e2ee.tls_passthrough.base_domain is the base domain suffix of E2EE share
	// URLs: with base_domain "e2ee.example.com", an SNI of
	// "<share code>.e2ee.example.com" routes to the share's agent. A literal
	// agent ID does not route (agent_reach.go).
	_ = flag.String("e2ee.tls_passthrough.base_domain", "",
		"base domain of E2EE share URLs, <share code>.<base domain> (e.g. e2ee.example.com)")

	// p2p.turn.*: TURN for `nfltr p2p call`. Off unless both the URLs and the
	// shared secret (env P2P_TURN_SECRET only, never a flag) are set; the
	// relay then issues per-call TURN REST credentials valid for p2p.turn.ttl.
	_ = flag.String("p2p.turn.urls", "",
		"comma-separated TURN URLs offered to p2p calls (turn:host:port?transport=udp); empty = no TURN")
	_ = flag.Duration("p2p.turn.ttl", time.Hour,
		"lifetime of a per-call TURN credential (1m-24h)")

	flag.Parse()
	loadFlags(viper.GetViper(), flag.CommandLine)

	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
}

// loadFlags gives viper the flags' values without viper.BindPFlags: with a
// bound flag set, every Get of a dotted key that no Set or set flag answers
// copies all ~200 flags into a fresh map to check that no flag shadows it
// (viper's isPathShadowedInFlatMap), about 35 KB of garbage per read, and
// the relay reads config on every proxied request. A set flag becomes an
// override and the rest become defaults, which keeps the precedence a key
// had (set flag > env > default) and the value Get returned, since each is
// read through a viper bound to the flag set alone.
func loadFlags(v *viper.Viper, fs *flag.FlagSet) {
	bound := viper.New()
	if err := bound.BindPFlags(fs); err != nil {
		panic(fmt.Errorf("could not bind flags: %v", err))
	}
	fs.VisitAll(func(f *flag.Flag) {
		if f.Changed {
			v.Set(f.Name, bound.Get(f.Name))
		} else {
			v.SetDefault(f.Name, bound.Get(f.Name))
		}
	})
}

// tcp.e2ee_policy values.
const (
	E2EERequire = "require"
	E2EEAllow   = "allow"
	E2EEForbid  = "forbid"
)

// ValidE2EEPolicy reports whether p is a tcp.e2ee_policy value.
func ValidE2EEPolicy(p string) bool {
	return p == E2EERequire || p == E2EEAllow || p == E2EEForbid
}
