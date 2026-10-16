package scrape

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// MetricType is the type declared by a "# TYPE" line.
type MetricType string

const (
	TypeCounter   MetricType = "counter"
	TypeGauge     MetricType = "gauge"
	TypeSummary   MetricType = "summary"
	TypeHistogram MetricType = "histogram"
	TypeUntyped   MetricType = "untyped"
)

// Sample is one parsed line of the text exposition format.
type Sample struct {
	Name   string
	Labels map[string]string // nil when the line has no labels
	Value  float64
	Type   MetricType
	// TimestampMs is meaningful only when HasTimestamp is set: zero is a valid
	// epoch value, so it cannot double as "absent".
	TimestampMs  int64
	HasTimestamp bool
}

// ParseResult is returned even when some lines are malformed: one bad line
// from a target must not cost us the other several hundred samples.
type ParseResult struct {
	Samples []Sample
	// Skipped counts malformed lines; FirstErr describes the first of them so
	// the log carries a concrete example instead of only a number.
	Skipped  int
	FirstErr error
}

// maxLineSize bounds a single line. bufio.Scanner's 64 KiB default is too
// tight for metrics with many long label values.
const maxLineSize = 1 << 20

// Parse reads the Prometheus text exposition format (version 0.0.4).
// The returned error is reserved for failures of the reader itself;
// malformed lines are reported through ParseResult.
func Parse(r io.Reader) (ParseResult, error) {
	var res ParseResult
	types := make(map[string]MetricType)

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineSize)

	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}

		var err error
		if line[0] == '#' {
			err = parseComment(line, types)
		} else {
			var s Sample
			s, err = parseSample(line)
			if err == nil {
				s.Type = resolveType(s.Name, types)
				res.Samples = append(res.Samples, s)
			}
		}
		if err != nil {
			res.Skipped++
			if res.FirstErr == nil {
				res.FirstErr = fmt.Errorf("line %d: %w", lineNo, err)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return res, fmt.Errorf("read exposition: %w", err)
	}
	return res, nil
}

// parseComment records "# TYPE" declarations. "# HELP" and free-form
// comments carry nothing we store.
func parseComment(line string, types map[string]MetricType) error {
	fields := strings.Fields(line[1:])
	if len(fields) == 0 || fields[0] != "TYPE" {
		return nil
	}
	if len(fields) != 3 {
		return errors.New("TYPE line must have a metric name and a type")
	}
	name, typ := fields[1], MetricType(fields[2])
	if !isValidMetricName(name) {
		return fmt.Errorf("invalid metric name %q in TYPE line", name)
	}
	switch typ {
	case TypeCounter, TypeGauge, TypeSummary, TypeHistogram, TypeUntyped:
		types[name] = typ
		return nil
	default:
		return fmt.Errorf("unknown metric type %q", typ)
	}
}

// resolveType maps a sample to its family's declared type. Summaries and
// histograms are declared under the base name but expose series with
// _sum/_count/_bucket suffixes, so those suffixes are stripped on a miss.
func resolveType(name string, types map[string]MetricType) MetricType {
	if t, ok := types[name]; ok {
		return t
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		base, ok := strings.CutSuffix(name, suffix)
		if !ok {
			continue
		}
		t := types[base]
		if t == TypeHistogram || (t == TypeSummary && suffix != "_bucket") {
			return t
		}
	}
	return TypeUntyped
}

// parseSample handles `name{label="value",...} value [timestamp_ms]`.
// It is a hand-written scanner rather than strings.Fields because label
// values may legally contain spaces, commas, braces and escaped quotes.
func parseSample(line string) (Sample, error) {
	var s Sample
	p := &cursor{s: line}

	s.Name = p.takeWhile(isNameChar)
	if !isValidMetricName(s.Name) {
		return s, fmt.Errorf("invalid metric name %q", s.Name)
	}

	if p.peek() == '{' {
		p.pos++
		labels, err := parseLabels(p)
		if err != nil {
			return s, fmt.Errorf("metric %s: %w", s.Name, err)
		}
		s.Labels = labels
	}

	if p.skipSpaces() == 0 {
		return s, fmt.Errorf("metric %s: expected whitespace before value", s.Name)
	}

	rawValue := p.takeWhile(isNotSpace)
	v, err := parseValue(rawValue)
	if err != nil {
		return s, fmt.Errorf("metric %s: %w", s.Name, err)
	}
	s.Value = v

	p.skipSpaces()
	if !p.done() {
		rawTS := p.takeWhile(isNotSpace)
		ts, err := strconv.ParseInt(rawTS, 10, 64)
		if err != nil {
			return s, fmt.Errorf("metric %s: invalid timestamp %q", s.Name, rawTS)
		}
		s.TimestampMs, s.HasTimestamp = ts, true
	}

	p.skipSpaces()
	if !p.done() {
		return s, fmt.Errorf("metric %s: unexpected trailing data %q", s.Name, p.s[p.pos:])
	}
	return s, nil
}

// parseLabels expects the cursor right after '{' and leaves it after '}'.
func parseLabels(p *cursor) (map[string]string, error) {
	labels := make(map[string]string)
	for {
		p.skipSpaces()
		if p.peek() == '}' {
			p.pos++
			return labels, nil
		}

		name := p.takeWhile(isLabelNameChar)
		if !isValidLabelName(name) {
			return nil, fmt.Errorf("invalid label name %q", name)
		}
		p.skipSpaces()
		if p.peek() != '=' {
			return nil, fmt.Errorf("label %s: expected '='", name)
		}
		p.pos++
		p.skipSpaces()

		value, err := parseLabelValue(p)
		if err != nil {
			return nil, fmt.Errorf("label %s: %w", name, err)
		}
		if _, dup := labels[name]; dup {
			return nil, fmt.Errorf("duplicate label %q", name)
		}
		labels[name] = value

		p.skipSpaces()
		switch p.peek() {
		case ',':
			// A trailing comma before '}' is allowed by the format.
			p.pos++
		case '}':
		default:
			return nil, fmt.Errorf("label %s: expected ',' or '}'", name)
		}
	}
}

// parseLabelValue reads a double-quoted value, decoding \\, \" and \n.
func parseLabelValue(p *cursor) (string, error) {
	if p.peek() != '"' {
		return "", errors.New("value must be double-quoted")
	}
	p.pos++

	var b strings.Builder
	for !p.done() {
		c := p.s[p.pos]
		p.pos++
		switch c {
		case '"':
			return b.String(), nil
		case '\\':
			if p.done() {
				return "", errors.New("unterminated escape sequence")
			}
			esc := p.s[p.pos]
			p.pos++
			switch esc {
			case '\\':
				b.WriteByte('\\')
			case '"':
				b.WriteByte('"')
			case 'n':
				b.WriteByte('\n')
			default:
				return "", fmt.Errorf("unknown escape sequence \\%c", esc)
			}
		default:
			b.WriteByte(c)
		}
	}
	return "", errors.New("unterminated value")
}

// parseValue accepts Go float syntax, which already covers the format's
// special values: +Inf, -Inf and NaN.
func parseValue(raw string) (float64, error) {
	if raw == "" {
		return 0, errors.New("missing value")
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid value %q", raw)
	}
	return v, nil
}

type cursor struct {
	s   string
	pos int
}

func (c *cursor) done() bool { return c.pos >= len(c.s) }

func (c *cursor) peek() byte {
	if c.done() {
		return 0
	}
	return c.s[c.pos]
}

func (c *cursor) takeWhile(ok func(byte) bool) string {
	start := c.pos
	for !c.done() && ok(c.s[c.pos]) {
		c.pos++
	}
	return c.s[start:c.pos]
}

func (c *cursor) skipSpaces() int {
	return len(c.takeWhile(func(b byte) bool { return b == ' ' || b == '\t' }))
}

func isNotSpace(b byte) bool { return b != ' ' && b != '\t' }

func isLabelNameChar(b byte) bool {
	return b == '_' || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9')
}

func isNameChar(b byte) bool { return isLabelNameChar(b) || b == ':' }

func isValidMetricName(name string) bool { return isValidName(name, isNameChar) }

func isValidLabelName(name string) bool { return isValidName(name, isLabelNameChar) }

func isValidName(name string, ok func(byte) bool) bool {
	if name == "" || ('0' <= name[0] && name[0] <= '9') {
		return false
	}
	for i := range len(name) {
		if !ok(name[i]) {
			return false
		}
	}
	return true
}
