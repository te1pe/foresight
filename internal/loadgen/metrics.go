package loadgen

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/te1pe/foresight/internal/scrape"
)

// snapshot is one read of a service's /metrics, parsed with foresight's
// own exposition parser: the load test measures foresight with foresight.
type snapshot struct {
	at      time.Time
	samples []scrape.Sample
}

func fetchSnapshot(ctx context.Context, client *http.Client, url string) (snapshot, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return snapshot{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return snapshot{}, fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return snapshot{}, fmt.Errorf("fetch %s: status %s", url, resp.Status)
	}
	res, err := scrape.Parse(resp.Body)
	if err != nil {
		return snapshot{}, fmt.Errorf("parse %s: %w", url, err)
	}
	return snapshot{at: time.Now(), samples: res.Samples}, nil
}

func matches(s scrape.Sample, name string, match map[string]string) bool {
	if s.Name != name {
		return false
	}
	for k, v := range match {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}

// sum adds up every series of name whose labels include match.
func (s snapshot) sum(name string, match map[string]string) float64 {
	var total float64
	for _, smp := range s.samples {
		if matches(smp, name, match) {
			total += smp.Value
		}
	}
	return total
}

type bucket struct {
	le    float64
	count float64
}

// buckets returns the cumulative histogram buckets of name (summed over
// series that include match), sorted by upper bound.
func (s snapshot) buckets(name string, match map[string]string) []bucket {
	byLE := map[float64]float64{}
	for _, smp := range s.samples {
		if !matches(smp, name+"_bucket", match) {
			continue
		}
		le, err := strconv.ParseFloat(smp.Labels["le"], 64)
		if err != nil {
			continue
		}
		byLE[le] += smp.Value
	}
	out := make([]bucket, 0, len(byLE))
	for le, c := range byLE {
		out = append(out, bucket{le: le, count: c})
	}
	slices.SortFunc(out, func(a, b bucket) int {
		switch {
		case a.le < b.le:
			return -1
		case a.le > b.le:
			return 1
		}
		return 0
	})
	return out
}

// delta subtracts an earlier read of the same histogram, leaving only the
// observations made during the measurement window.
func delta(after, before []bucket) []bucket {
	prev := make(map[float64]float64, len(before))
	for _, b := range before {
		prev[b.le] = b.count
	}
	out := make([]bucket, len(after))
	for i, b := range after {
		out[i] = bucket{le: b.le, count: b.count - prev[b.le]}
	}
	return out
}

// quantile estimates the q-quantile from cumulative buckets the way
// Prometheus' histogram_quantile does: find the bucket holding the rank and
// interpolate linearly inside it. The answer is only as precise as the
// bucket layout. NaN means no observations.
func quantile(q float64, buckets []bucket) float64 {
	if len(buckets) == 0 {
		return math.NaN()
	}
	total := buckets[len(buckets)-1].count
	if total == 0 {
		return math.NaN()
	}
	rank := q * total
	lowerLE, lowerCount := 0.0, 0.0
	for _, b := range buckets {
		if b.count >= rank {
			if math.IsInf(b.le, 1) {
				// Beyond the largest finite bound: all we know is "more than".
				return lowerLE
			}
			if b.count == lowerCount {
				return b.le
			}
			return lowerLE + (b.le-lowerLE)*(rank-lowerCount)/(b.count-lowerCount)
		}
		lowerLE, lowerCount = b.le, b.count
	}
	return lowerLE
}
