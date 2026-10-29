# Security

## Reporting a vulnerability

Report vulnerabilities privately through GitHub's private vulnerability
reporting: the repository's **Security** tab, **Report a vulnerability**
(a draft security advisory only the maintainers see). Please do not open a
public issue. Include the version (tag or commit), what an attacker
controls, and a reproduction if you have one. We aim to acknowledge a
report within a week, and to publish a fix and an advisory (with a CVE
where one applies) once a fixed release is tagged.

Supported versions: the latest minor release (v0.x: minor versions may
change the API, see [CHANGELOG.md](CHANGELOG.md)). Fixes are not
backported to older minors.

## Scope

relay-core is a relay's session core: agents connect to it over gRPC with
a credential, clients open sessions to those agents through it. What it
guarantees, and what it leaves to the operator:

- **Transport security is the operator's.** The proxy and gRPC ports take
  the operator's certificate (`tls.enable`, `tls.cert_file`,
  `tls.key_file`); there is no ACME. Without TLS, `nfltr-relay-core`
  listens on loopback only and refuses any other address.
- **Authentication is a provider chain** (`core/identity`): the key file
  (`keyfile`), or providers of your own. A provider can only authenticate
  or not; it never sees a target, so it cannot widen what a principal
  reaches. Keys are held hashed (SHA-256), never in plaintext.
- **Reach is per owner.** A principal reaches only agents under its own
  owner; an unknown credential is refused (gRPC Unauthenticated, HTTP 401).
- **End-to-end encryption** of TCP sessions is the ends' (TLS ended at the
  agent, the client pinning the agent's certificate); `tcp.e2ee_policy`
  makes the relay require, allow or forbid it. The relay then carries
  ciphertext only.
- **Untrusted input is fuzzed**: share codes (`FuzzValidCode`), the SNI
  parser (`FuzzExtractSNI`, `FuzzExtractSNIFromTLSClient`,
  `FuzzParseSNIExtension`) and name templates (`FuzzTemplate`);
  `make fuzz-short` runs them.
