package alert

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	chstore "github.com/te1pe/foresight/internal/storage/clickhouse"
	"github.com/te1pe/foresight/internal/storage/postgres"
)

// ruleTitles makes messages readable; an unknown rule falls back to its
// name, so a new rule never needs a change here to be delivered.
var ruleTitles = map[string]string{
	"disk_fill_forecast":      "Диск заполняется",
	"disk_usage_rate_anomaly": "Резко изменилась скорость заполнения диска",
	"goroutine_leak":          "Похоже на утечку горутин",
	"crdt_journal_growth":     "Журнал CRDT растёт без компакции",
}

func title(rule string) string {
	if t, ok := ruleTitles[rule]; ok {
		return t
	}
	return rule
}

// seriesName is "target label=value ...", with empty labels dropped
// (node_exporter exports device_error="" on every filesystem).
func seriesName(target string, labels map[string]string) string {
	var sb strings.Builder
	sb.WriteString(target)
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if labels[k] != "" {
			fmt.Fprintf(&sb, " %s=%s", k, labels[k])
		}
	}
	return sb.String()
}

// summary is the one-line state of a firing detection; it is stored on the
// incident and refreshed every minute while the incident stays open.
func summary(d chstore.Detection) string {
	bytes := strings.HasSuffix(d.Name, "_bytes")
	switch {
	case d.Kind == "forecast" && d.ETA != nil:
		s := "закончится через " + formatDuration(*d.ETA)
		if bytes {
			s = fmt.Sprintf("заполнится через %s (свободно %s, убывает на %s/мин)",
				formatDuration(*d.ETA), formatBytes(d.Expected), formatBytes(-d.Slope*60))
		}
		return s
	case d.Kind == "growth":
		return fmt.Sprintf("растёт на %.0f в час, сейчас около %.0f (t = %.1f, по %d точкам)",
			d.Slope*3600, d.Expected, d.Score, d.Points)
	case d.Kind == "anomaly":
		if bytes {
			return fmt.Sprintf("свободное место меняется на %s/мин, обычно %s/мин (z = %.1f)",
				formatSignedBytes(d.Observed*60), formatSignedBytes(d.Expected*60), d.Score)
		}
		return fmt.Sprintf("значение %.3g, обычно %.3g (z = %.1f)", d.Observed, d.Expected, d.Score)
	default:
		return d.Status
	}
}

func openedMessage(d chstore.Detection) string {
	return fmt.Sprintf("🔴 %s\n%s\n%s", title(d.Rule), seriesName(d.Target, d.Labels), summary(d))
}

func resolvedMessage(inc postgres.Incident, reason string, at time.Time) string {
	why := "условие больше не выполняется"
	if reason == postgres.ResolveNoData {
		why = "ряд перестал оцениваться"
	}
	return fmt.Sprintf("✅ Решено: %s\n%s\n%s, инцидент длился %s",
		title(inc.Rule), seriesName(inc.Target, inc.Labels), why, formatDuration(at.Sub(inc.OpenedAt)))
}

// formatDuration renders "3 ч 20 мин": minutes are the resolution of the
// whole pipeline, so seconds would be false precision.
func formatDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	if d < time.Minute {
		return "меньше минуты"
	}
	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour
	hours := d / time.Hour
	minutes := (d - hours*time.Hour) / time.Minute

	var parts []string
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%d д", days))
	}
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%d ч", hours))
	}
	if minutes > 0 {
		parts = append(parts, fmt.Sprintf("%d мин", minutes))
	}
	return strings.Join(parts, " ")
}

func formatBytes(b float64) string {
	units := []string{"Б", "КиБ", "МиБ", "ГиБ", "ТиБ"}
	i := 0
	for math.Abs(b) >= 1024 && i < len(units)-1 {
		b /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%.0f %s", b, units[i])
	}
	return fmt.Sprintf("%.1f %s", b, units[i])
}

func formatSignedBytes(b float64) string {
	if b > 0 {
		return "+" + formatBytes(b)
	}
	return formatBytes(b)
}
