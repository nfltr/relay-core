// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// relayFlags registers the relay's flags on a fresh flag set, as
// SetDefaultFlags does, and parses args.
func relayFlags(t *testing.T, args ...string) *flag.FlagSet {
	t.Helper()
	saved := flag.CommandLine
	defer func() { flag.CommandLine = saved }()
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	savedArgs := os.Args
	defer func() { os.Args = savedArgs }()
	os.Args = append([]string{os.Args[0]}, args...)
	defer viper.Reset()
	SetDefaultFlags()
	return flag.CommandLine
}

// newViper is a viper set up as SetDefaultFlags sets up the global one,
// with the flags loaded by load, then a config file.
func newViper(t *testing.T, fs *flag.FlagSet, load func(*viper.Viper, *flag.FlagSet), configYAML string) *viper.Viper {
	t.Helper()
	v := viper.New()
	load(v, fs)
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.SetConfigType("yaml")
	if err := v.ReadConfig(strings.NewReader(configYAML)); err != nil {
		t.Fatal(err)
	}
	return v
}

func bindPFlags(v *viper.Viper, fs *flag.FlagSet) {
	if err := v.BindPFlags(fs); err != nil {
		panic(err)
	}
}

// TestLoadFlagsMatchesBindPFlags: every key reads the same value through
// loadFlags as through viper.BindPFlags, whichever layer answers it: an
// explicitly set flag (also one set to its default value) over the env
// (AutomaticEnv with the . and - replacer) over a config file over a flag's
// default, a key only the env sets, and a runtime viper.Set over all of them.
func TestLoadFlagsMatchesBindPFlags(t *testing.T) {
	fs := relayFlags(t,
		"--internal.port=7002",           // set flag, env also sets it
		"--http.server.fanout_workers=1", // set to its default value, env also sets it
		"--loopback=127.0.0.2",           // set flag, config also sets it
		"--grpc-port=9443",               // a hyphenated name
	)
	t.Setenv("INTERNAL_PORT", "7001")
	t.Setenv("HTTP_SERVER_FANOUT_WORKERS", "8")
	t.Setenv("HTTP_SHARE_DOMAIN", "env.example")     // env over config and default
	t.Setenv("PROXY_MAX_IDLE_CONNS_PER_HOST", "77")  // env over default
	t.Setenv("NOT_A_FLAG_ONLY_ENV", "from-env")      // a key only the env sets
	t.Setenv("HTTP_DIRECT_AGENT_SUBDOMAINS", "true") // bool from env
	config := strings.Join([]string{
		"http:",
		"  share_domain: config.example",
		"  api_type_prefixes: [a, b]",
		"loopback: 10.0.0.1",
		"request:",
		"  timeout:",
		"    fileop_path_prefix: /cfg", // config over default
	}, "\n")

	want := newViper(t, fs, bindPFlags, config)
	got := newViper(t, fs, loadFlags, config)

	keys := []string{"not_a_flag_only_env", "not.a.key"}
	fs.VisitAll(func(f *flag.Flag) { keys = append(keys, f.Name) })
	check := func(stage string) {
		t.Helper()
		for _, k := range keys {
			if g, w := got.Get(k), want.Get(k); !reflect.DeepEqual(g, w) {
				t.Errorf("%s: %s = %#v, want %#v (as with BindPFlags)", stage, k, g, w)
			}
		}
	}
	check("startup")

	for k, w := range map[string]any{
		"internal.port":                      "7002",
		"http.server.fanout_workers":         1,
		"loopback":                           "127.0.0.2",
		"http.share_domain":                  "env.example",
		"proxy.max_idle_conns_per_host":      "77",
		"not_a_flag_only_env":                "from-env",
		"request.timeout.fileop_path_prefix": "/cfg",
		"http.direct_agent_subdomains":       "true",
		"grpc-port":                          "9443",
	} {
		if g := got.Get(k); !reflect.DeepEqual(g, w) {
			t.Errorf("%s = %#v, want %#v", k, g, w)
		}
	}

	for _, v := range []*viper.Viper{want, got} {
		v.Set("internal.port", "0")
		v.Set("http.share_domain", "")
	}
	check("after a runtime viper.Set")
	if g := got.GetString("internal.port"); g != "0" {
		t.Errorf("runtime viper.Set over a set flag: internal.port = %q, want 0", g)
	}
}

// TestDottedKeyReadDoesNotCopyTheFlagSet guards the request path: the relay
// reads dotted keys per request, and with viper.BindPFlags each such read
// copied every flag into a new map (~35 KB for the relay's flags).
func TestDottedKeyReadDoesNotCopyTheFlagSet(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	SetDefaultFlags()

	const reads = 100
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range reads {
		_ = viper.GetInt64("http.server.max_request_body_size")
	}
	runtime.ReadMemStats(&after)
	if perRead := (after.TotalAlloc - before.TotalAlloc) / reads; perRead > 1024 {
		t.Fatalf("a dotted config read allocates %d bytes, want well under 1 KiB (no copy of the flag set)", perRead)
	}
}

// The relay keeps no request or response bodies: the traffic inspector is
// the agent's (nfltr http --record), so no capture.* flag exists.
func TestNoTrafficCaptureFlags(t *testing.T) {
	relayFlags(t).VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "capture.") {
			t.Errorf("flag %q: the relay must not capture traffic", f.Name)
		}
	})
}
