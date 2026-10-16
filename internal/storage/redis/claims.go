// Package redis deduplicates notifications across retries and alerter
// replicas.
package redis

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Claims guards a side effect that cannot be rolled back (a sent Telegram
// message) with a two-state key:
//
//	absent  -> nobody has tried yet
//	pending -> someone is sending right now; expires after a short TTL.
//	           The value carries a random token identifying the claimant.
//	sent    -> done; kept long enough to outlive any retry
//
// Claiming sets "pending" with SET NX, so only one caller proceeds. If that
// caller dies before sending, "pending" expires and the send is retried:
// delivery is at-least-once, and a duplicate can only happen if a process
// dies in the instant between a successful send and MarkSent.
type Claims struct {
	client *goredis.Client
}

type ClaimResult int

const (
	Acquired    ClaimResult = iota // proceed and send
	AlreadySent                    // done earlier; only record it
	InProgress                     // someone else holds the claim; skip for now
)

// Claim is a held claim; its token proves ownership on Release.
type Claim struct {
	Result ClaimResult
	token  string
}

const (
	pendingPrefix = "pending:"
	sentValue     = "sent"
)

// releaseScript deletes the key only if it still holds our pending token.
// A plain DEL is unsafe: if our pending TTL expired and another alerter
// claimed the key, DEL would remove *its* claim and let a third one send
// the same message. GET-compare-DEL must be atomic, hence Lua.
var releaseScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

func New(addr string) *Claims {
	return &Claims{client: goredis.NewClient(&goredis.Options{Addr: addr})}
}

func (c *Claims) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

func (c *Claims) Close() error { return c.client.Close() }

func (c *Claims) Claim(ctx context.Context, key string, pendingTTL time.Duration) (Claim, error) {
	token := pendingPrefix + rand.Text()
	err := c.client.SetArgs(ctx, key, token, goredis.SetArgs{Mode: "NX", TTL: pendingTTL}).Err()
	if err == nil {
		return Claim{Result: Acquired, token: token}, nil
	}
	if !errors.Is(err, goredis.Nil) {
		return Claim{}, fmt.Errorf("claim %s: %w", key, err)
	}

	// The key exists. It may also have expired between the two commands;
	// then report InProgress and let the next round claim it cleanly.
	val, err := c.client.Get(ctx, key).Result()
	switch {
	case errors.Is(err, goredis.Nil):
		return Claim{Result: InProgress}, nil
	case err != nil:
		return Claim{}, fmt.Errorf("read claim %s: %w", key, err)
	case val == sentValue:
		return Claim{Result: AlreadySent}, nil
	case strings.HasPrefix(val, pendingPrefix):
		return Claim{Result: InProgress}, nil
	default:
		return Claim{}, fmt.Errorf("claim %s: unexpected value %q", key, val)
	}
}

func (c *Claims) MarkSent(ctx context.Context, key string, ttl time.Duration) error {
	if err := c.client.Set(ctx, key, sentValue, ttl).Err(); err != nil {
		return fmt.Errorf("mark %s sent: %w", key, err)
	}
	return nil
}

// Release gives up our claim after a failed send so the next round can
// retry right away instead of waiting for the pending TTL. A claim that is
// no longer ours (expired and taken over) is left alone.
func (c *Claims) Release(ctx context.Context, key string, claim Claim) error {
	if claim.Result != Acquired {
		return nil
	}
	if err := releaseScript.Run(ctx, c.client, []string{key}, claim.token).Err(); err != nil {
		return fmt.Errorf("release %s: %w", key, err)
	}
	return nil
}
