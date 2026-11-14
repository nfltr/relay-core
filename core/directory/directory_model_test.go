// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// A model-based test of the session records: each walk of spec/Directory.tla
// (spec/testdata/directory-walks.ndjson, made by scripts/spec.sh walks) is
// replayed against the real store, ClaimSession, PutSession with renew and
// DeleteSession on a MemoryCache through two inventories (the dispatcher's
// and the streamer's record), and after every step the records, the Seq
// counters, each session's Seq and state (a renewal's ErrSuperseded) and
// whether the agent is in the nf1 index of the rid its dispatcher record
// declares must be the model's (Prune: a read of that index).

const walkAgent = "agent-1"

type walkState struct {
	Act struct {
		N string `json:"n"` // Init, Claim, ClaimNoSeq, Renew, End, Expire, Loss, Prune
		P string `json:"p"`
		K string `json:"k"`
		I int    `json:"i"` // 1-based session
	} `json:"act"`
	Rec  map[string]int   `json:"rec"`
	Idx  bool             `json:"idx"`
	Ctr  map[string]int64 `json:"ctr"`
	Sess []struct {
		Seq int64  `json:"seq"`
		Key string `json:"key"`
		Pod string `json:"pod"`
		St  string `json:"st"`
	} `json:"sess"`
}

func readWalks(t *testing.T) [][]walkState {
	t.Helper()
	f, err := os.Open("../../spec/testdata/directory-walks.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var walks [][]walkState
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var s walkState
		d := json.NewDecoder(bytes.NewReader(sc.Bytes()))
		d.DisallowUnknownFields()
		if err := d.Decode(&s); err != nil {
			t.Fatalf("walk line: %v", err)
		}
		if s.Act.N == "Init" {
			walks = append(walks, nil)
		}
		walks[len(walks)-1] = append(walks[len(walks)-1], s)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return walks
}

var walkKeys = map[string]func(string) string{"d": GetDispatcherCacheKey, "s": GetStreamerCacheKey}

// walkRID is the nf1 rendezvous id the dispatcher's sessions declare.
var walkRID = [16]byte{0xab, 1}

// walkServices is what a session of record key k declares.
func walkServices(k string) string {
	if k == "d" {
		return "clip=tcp+e2ee;nf1=ab010000000000000000000000000000"
	}
	return ""
}

func walkPod(p string) string { return "10.0.0." + strings.TrimPrefix(p, "p") }

type walkSession struct {
	s   Session
	key string
	pod string
	st  string
}

// replayWalk runs one walk against cache (reset empties it: the store's
// loss) and returns the first step where the store and the model differ.
func replayWalk(t *testing.T, cache Cache, reset func(), walk []walkState) error {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 3, 1, 0, 0, 0, time.UTC)
	stores := map[string]*ipInventory{}
	for k, fn := range walkKeys {
		inv, _ := NewIPInventory(testLogger(), cache, time.Second, "127.0.0.1", fn)
		stores[k] = inv.(*ipInventory)
	}
	var sess []*walkSession
	for n, m := range walk {
		a := m.Act
		var err error
		switch a.N {
		case "Init":
		case "Claim":
			t.Setenv("POD_IP", walkPod(a.P))
			var s Session
			s, err = stores[a.K].ClaimSession(ctx, walkAgent, Session{Since: t0.Add(time.Duration(a.I) * time.Second), Services: walkServices(a.K)}, nil, nil, time.Minute)
			sess = append(sess, &walkSession{s: s, key: a.K, pod: a.P, st: "live"})
		case "ClaimNoSeq":
			// The counter step fails (the store unreachable): no Seq, no
			// record; ConnectTCP goes on with the session (finding 37).
			t.Setenv("POD_IP", walkPod(a.P))
			inv, _ := NewIPInventory(testLogger(), failIncr{cache}, time.Second, "127.0.0.1", walkKeys[a.K])
			s, cerr := inv.(*ipInventory).ClaimSession(ctx, walkAgent, Session{Since: t0.Add(time.Duration(a.I) * time.Second)}, nil, nil, time.Minute)
			if cerr == nil || s.Seq != 0 {
				return fmt.Errorf("step %d: a claim whose counter failed: Seq %d, err %v", n, s.Seq, cerr)
			}
			sess = append(sess, &walkSession{s: s, key: a.K, pod: a.P, st: "live"})
		case "Renew":
			ws := sess[a.I-1]
			t.Setenv("POD_IP", walkPod(ws.pod))
			if err = stores[ws.key].PutSession(ctx, walkAgent, ws.s, true, nil, nil, time.Minute); errors.Is(err, ErrSuperseded) {
				ws.st, err = "sup", nil
			}
		case "End":
			ws := sess[a.I-1]
			t.Setenv("POD_IP", walkPod(ws.pod))
			err = stores[ws.key].DeleteSession(ctx, walkAgent, ws.s)
			ws.st = "end"
		case "Expire":
			_, err = cache.Del(ctx, walkKeys[a.K](walkAgent))
		case "Loss":
			reset()
		case "Prune":
			_, err = stores["d"].NF1Agents(ctx, walkRID)
		default:
			return fmt.Errorf("step %d: unknown action %q", n, a.N)
		}
		if err != nil {
			return fmt.Errorf("step %d %+v: %w", n, a, err)
		}
		if err := compareWalkState(ctx, cache, stores["d"], m, sess, t0); err != nil {
			return fmt.Errorf("step %d %+v: %w", n, a, err)
		}
	}
	return nil
}

func compareWalkState(ctx context.Context, cache Cache, d *ipInventory, m walkState, sess []*walkSession, t0 time.Time) error {
	members, err := cache.SInter(ctx, d.nf1Key(walkRID))
	if err != nil {
		return err
	}
	if in := slices.Contains(members, walkAgent); in != m.Idx {
		return fmt.Errorf("nf1 index membership %v, model %v", in, m.Idx)
	}
	if len(sess) != len(m.Sess) {
		return fmt.Errorf("%d sessions, model %d", len(sess), len(m.Sess))
	}
	for i, ws := range sess {
		if ws.s.Seq != m.Sess[i].Seq || ws.st != m.Sess[i].St {
			return fmt.Errorf("session %d: Seq %d %s, model Seq %d %s", i+1, ws.s.Seq, ws.st, m.Sess[i].Seq, m.Sess[i].St)
		}
	}
	for k, fn := range walkKeys {
		key := fn(walkAgent)
		v, err := cache.Get(ctx, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if j := m.Rec[k]; j == 0 {
			if v != nil {
				return fmt.Errorf("record %s: %+v, model none", k, *v)
			}
		} else if want := m.Sess[j-1]; v == nil || v.PodIP != walkPod(want.Pod) || v.Seq != want.Seq || v.Since != t0.Add(time.Duration(j)*time.Second).UnixNano() {
			return fmt.Errorf("record %s: %+v, model session %d %+v", k, v, j, want)
		}
		c, err := cache.Get(ctx, seqKey(key))
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		var n int64
		if c != nil {
			if n, err = strconv.ParseInt(c.PodIP, 10, 64); err != nil {
				return err
			}
		}
		if n != m.Ctr[k] {
			return fmt.Errorf("counter %s: %d, model %d", k, n, m.Ctr[k])
		}
	}
	return nil
}

func walkEnv(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
}

func resetMemory(mc *MemoryCache) func() {
	return func() {
		mc.mu.Lock()
		defer mc.mu.Unlock()
		mc.kv, mc.sets = map[string]memoryEntry{}, map[string]map[string]struct{}{}
	}
}

func TestDirectoryModelWalks(t *testing.T) {
	walkEnv(t)
	walks := readWalks(t)
	seen := map[string]int{}
	for i, w := range walks {
		mc := NewMemoryCache()
		if err := replayWalk(t, mc, resetMemory(mc), w); err != nil {
			t.Fatalf("walk %d: %v", i+1, err)
		}
		for _, s := range w {
			seen[s.Act.N]++
			for _, x := range s.Sess {
				seen[x.St]++
			}
		}
	}
	// The walks must take every step and reach a superseded session.
	for _, a := range []string{"Claim", "ClaimNoSeq", "Renew", "End", "Expire", "Loss", "Prune", "sup"} {
		if seen[a] == 0 {
			t.Errorf("no walk takes %s: regenerate spec/testdata/directory-walks.ndjson (scripts/spec.sh walks)", a)
		}
	}
}

// failIncr is a store whose counter step fails.
type failIncr struct{ Cache }

func (failIncr) Incr(context.Context, string, int64, time.Duration) (int64, error) {
	return 0, errors.New("store unreachable")
}

// The replay is not vacuous: a store with the bugs the model's mutants put
// back diverges from some walk. sharedCounter is finding 34 (one Seq
// counter for both records); blindCounter finding 13 (a renewal that
// finds no record does not read the counter).
type bugCache struct {
	Cache
	sharedCounter, blindCounter bool
}

func (b bugCache) mapKey(k string) string {
	if b.sharedCounter && strings.HasPrefix(k, "nfltr:agent-session-seq:") {
		return seqKey(GetDispatcherCacheKey(walkAgent))
	}
	return k
}

func (b bugCache) Get(ctx context.Context, k string) (*Value, error) {
	return b.Cache.Get(ctx, b.mapKey(k))
}

func (b bugCache) Incr(ctx context.Context, k string, by int64, ttl time.Duration) (int64, error) {
	return b.Cache.Incr(ctx, b.mapKey(k), by, ttl)
}

func (b bugCache) SetAddIf(ctx context.Context, key, ctr string, match func(*Value, int64) bool, v Value, exp time.Duration, member string, sets ...string) (bool, error) {
	if b.blindCounter {
		ctr = ""
	}
	return b.Cache.SetAddIf(ctx, key, b.mapKey(ctr), match, v, exp, member, sets...)
}

func TestDirectoryModelWalksCatchPlantedBugs(t *testing.T) {
	walkEnv(t)
	walks := readWalks(t)
	for _, bug := range []bugCache{{sharedCounter: true}, {blindCounter: true}} {
		diverged := false
		for _, w := range walks {
			mc := NewMemoryCache()
			bug.Cache = mc
			if replayWalk(t, bug, resetMemory(mc), w) != nil {
				diverged = true
				break
			}
		}
		if !diverged {
			t.Errorf("no walk diverges from a store with %+v", bug)
		}
	}
}
