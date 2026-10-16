package redis

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// Same version as deploy/docker-compose.yml.
const redisImage = "redis:8.8-alpine"

var addr string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("skipping redis integration tests in -short mode")
		os.Exit(0)
	}
	os.Exit(runWithRedis(m))
}

func runWithRedis(m *testing.M) int {
	ctx := context.Background()
	ctr, err := tcredis.Run(ctx, redisImage)
	defer func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			fmt.Fprintln(os.Stderr, "terminate container:", err)
		}
	}()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start redis container:", err)
		return 1
	}
	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connection string:", err)
		return 1
	}
	addr = strings.TrimPrefix(uri, "redis://")
	return m.Run()
}

func newClaims(t *testing.T) *Claims {
	t.Helper()
	c := New(addr)
	t.Cleanup(func() { c.Close() })
	return c
}

func TestClaimLifecycle(t *testing.T) {
	c := newClaims(t)
	ctx := t.Context()
	key := "test:" + t.Name()

	first, err := c.Claim(ctx, key, time.Minute)
	if err != nil || first.Result != Acquired {
		t.Fatalf("first claim: %+v, %v", first, err)
	}
	if second, _ := c.Claim(ctx, key, time.Minute); second.Result != InProgress {
		t.Errorf("claim while pending = %v, want InProgress", second.Result)
	}

	if err := c.MarkSent(ctx, key, time.Hour); err != nil {
		t.Fatalf("mark sent: %v", err)
	}
	if after, _ := c.Claim(ctx, key, time.Minute); after.Result != AlreadySent {
		t.Errorf("claim after send = %v, want AlreadySent", after.Result)
	}
}

func TestReleaseAllowsRetry(t *testing.T) {
	c := newClaims(t)
	ctx := t.Context()
	key := "test:" + t.Name()

	claim, _ := c.Claim(ctx, key, time.Minute)
	if err := c.Release(ctx, key, claim); err != nil {
		t.Fatalf("release: %v", err)
	}
	if retry, _ := c.Claim(ctx, key, time.Minute); retry.Result != Acquired {
		t.Errorf("claim after release = %v, want Acquired", retry.Result)
	}
}

// The scenario a plain DEL gets wrong: our claim expired, another alerter
// claimed the key, and only then does our failed send call Release.
func TestReleaseDoesNotStealAnotherClaim(t *testing.T) {
	c := newClaims(t)
	ctx := t.Context()
	key := "test:" + t.Name()

	stale, _ := c.Claim(ctx, key, 100*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	fresh, _ := c.Claim(ctx, key, time.Minute)
	if fresh.Result != Acquired {
		t.Fatalf("claim after expiry = %v, want Acquired", fresh.Result)
	}

	if err := c.Release(ctx, key, stale); err != nil {
		t.Fatalf("stale release: %v", err)
	}
	if third, _ := c.Claim(ctx, key, time.Minute); third.Result != InProgress {
		t.Errorf("after a stale release the fresh claim must survive; got %v", third.Result)
	}
}

func TestOnlyOneConcurrentClaimWins(t *testing.T) {
	c := newClaims(t)
	key := "test:" + t.Name()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		wins int
	)
	for range 16 {
		wg.Go(func() {
			cl, err := c.Claim(t.Context(), key, time.Minute)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if cl.Result == Acquired {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if wins != 1 {
		t.Errorf("%d concurrent claims acquired, want 1", wins)
	}
}
