// float_dedup — Telegraf execd processor
//
// Drops metrics whose field values haven't changed beyond a threshold since
// the last reported value, unless the heartbeat interval has elapsed.
//
// Replaces:  processors.starlark (float×100 hack) + processors.dedup
//
// Float fields:    pass if |new - last| >= epsilon  OR  age >= heartbeat
// Int/bool fields: pass if value changed             OR  age >= heartbeat
// String fields:   always pass
//
// Usage:
//   float_dedup [--config float_dedup.toml] [--epsilon 0.5] [--heartbeat 60s]
//
// Telegraf filtered config:
//   [[processors.execd]]
//     command     = ["/path/to/float_dedup", "--config", "/path/to/float_dedup.toml"]
//     signal      = "STDIN_CLOSED"
//     order       = 1
//     data_format = "influx"

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	lp "github.com/influxdata/line-protocol/v2/lineprotocol"
)

// ── Config ────────────────────────────────────────────────────────────────────

type Rule struct {
	Pattern string  `toml:"pattern"` // filepath.Match glob on point_name tag
	Epsilon float64 `toml:"epsilon"` // absolute change threshold
}

type Config struct {
	Heartbeat      string  `toml:"heartbeat"`
	DefaultEpsilon float64 `toml:"default_epsilon"`
	Rules          []Rule  `toml:"rule"`
	hbDur          time.Duration
}

func loadConfig(path, flagHB string, flagEpsilon float64) (*Config, error) {
	cfg := Config{Heartbeat: "60s", DefaultEpsilon: 0.5}
	if path != "" {
		if _, err := toml.DecodeFile(path, &cfg); err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
	}
	if flagHB != "" {
		cfg.Heartbeat = flagHB
	}
	if flagEpsilon >= 0 {
		cfg.DefaultEpsilon = flagEpsilon
	}
	d, err := time.ParseDuration(cfg.Heartbeat)
	if err != nil {
		return nil, fmt.Errorf("heartbeat %q: %w", cfg.Heartbeat, err)
	}
	cfg.hbDur = d
	return &cfg, nil
}

func (c *Config) epsilonFor(pointName string) float64 {
	for _, r := range c.Rules {
		if ok, _ := filepath.Match(r.Pattern, pointName); ok {
			return r.Epsilon
		}
	}
	return c.DefaultEpsilon
}

// ── State ─────────────────────────────────────────────────────────────────────

type entry struct {
	fval float64
	ival int64
	sval string // for string fields — exact-match dedup
	ts   time.Time
}

var store = make(map[string]*entry)

type tagKV struct{ k, v []byte }

func seriesKey(meas []byte, tags []tagKV, field []byte) string {
	sorted := make([]tagKV, len(tags))
	copy(sorted, tags)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].k, sorted[j].k) < 0
	})
	var sb strings.Builder
	sb.Write(meas)
	for _, t := range sorted {
		sb.WriteByte(',')
		sb.Write(t.k)
		sb.WriteByte('=')
		sb.Write(t.v)
	}
	sb.WriteByte(' ')
	sb.Write(field)
	return sb.String()
}

func tagVal(tags []tagKV, name string) string {
	for _, t := range tags {
		if string(t.k) == name {
			return string(t.v)
		}
	}
	return ""
}

// ── Filter ────────────────────────────────────────────────────────────────────

func passes(key string, val lp.Value, ts time.Time, epsilon float64, hb time.Duration) bool {
	e, exists := store[key]
	if !exists {
		return true
	}
	if hb > 0 && ts.Sub(e.ts) >= hb {
		return true
	}
	switch val.Kind() {
	case lp.Float:
		return math.Abs(val.FloatV()-e.fval) >= epsilon
	case lp.Int:
		return val.IntV() != e.ival
	case lp.Uint:
		return int64(val.UintV()) != e.ival
	case lp.Bool:
		bv := int64(0)
		if val.BoolV() {
			bv = 1
		}
		return bv != e.ival
	case lp.String:
		// String fields: exact-match dedup — only forward on value change.
		// This covers status enums, firmware versions, mode labels etc.
		// Strings change rarely; suppressing repeats saves TSM writes without
		// losing any information (no heartbeat needed — strings don't aggregate).
		return val.StringV() != e.sval
	}
	return true
}

func updateStore(key string, val lp.Value, ts time.Time) {
	e, exists := store[key]
	if !exists {
		e = &entry{}
		store[key] = e
	}
	e.ts = ts
	switch val.Kind() {
	case lp.Float:
		e.fval = val.FloatV()
	case lp.Int:
		e.ival = val.IntV()
	case lp.Uint:
		e.ival = int64(val.UintV())
	case lp.Bool:
		if val.BoolV() {
			e.ival = 1
		} else {
			e.ival = 0
		}
	case lp.String:
		e.sval = val.StringV()
	}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	configPath := flag.String("config", "", "TOML config file path")
	flagEps    := flag.Float64("epsilon", -1, "default epsilon (overrides config)")
	flagHB     := flag.String("heartbeat", "", "heartbeat e.g. 60s (overrides config)")
	flag.Parse()

	cfg, err := loadConfig(*configPath, *flagHB, *flagEps)
	if err != nil {
		log.Fatalf("float_dedup: %v", err)
	}
	log.Printf("float_dedup: heartbeat=%s default_epsilon=%.4f rules=%d",
		cfg.Heartbeat, cfg.DefaultEpsilon, len(cfg.Rules))

	in  := bufio.NewReaderSize(os.Stdin, 1<<20)
	out := bufio.NewWriterSize(os.Stdout, 1<<20)
	dec := lp.NewDecoder(in)

	var enc lp.Encoder
	enc.SetPrecision(lp.Nanosecond)

	var (
		passed  uint64
		dropped uint64
	)

	type fieldEntry struct {
		key []byte
		val lp.Value
	}

	for dec.Next() {
		meas, err := dec.Measurement()
		if err != nil {
			log.Printf("float_dedup: measurement: %v", err)
			continue
		}
		measCopy := make([]byte, len(meas))
		copy(measCopy, meas)

		// Collect tags
		var tags []tagKV
		for {
			k, v, err := dec.NextTag()
			if err != nil {
				log.Printf("float_dedup: tag: %v", err)
				break
			}
			if k == nil {
				break
			}
			kc := make([]byte, len(k))
			vc := make([]byte, len(v))
			copy(kc, k)
			copy(vc, v)
			tags = append(tags, tagKV{kc, vc})
		}

		pointName := tagVal(tags, "point_name")
		epsilon   := cfg.epsilonFor(pointName)

		// Collect fields, filter per-field
		var keepFields []fieldEntry
		var metricTS time.Time

		for {
			k, v, err := dec.NextField()
			if err != nil {
				log.Printf("float_dedup: field: %v", err)
				break
			}
			if k == nil {
				break
			}
			kc := make([]byte, len(k))
			copy(kc, k)

			ts, terr := dec.Time(lp.Nanosecond, time.Now())
			if terr != nil {
				ts = time.Now()
			}
			metricTS = ts

			key := seriesKey(measCopy, tags, kc)
			if passes(key, v, ts, epsilon, cfg.hbDur) {
				updateStore(key, v, ts)
				keepFields = append(keepFields, fieldEntry{kc, v})
				passed++
			} else {
				dropped++
			}
		}

		if len(keepFields) == 0 {
			tags = tags[:0]
			keepFields = keepFields[:0]
			continue
		}

		// Re-encode and write — encoder requires tags sorted by key
		sort.Slice(tags, func(i, j int) bool {
			return bytes.Compare(tags[i].k, tags[j].k) < 0
		})
		enc.Reset()
		enc.StartLineRaw(measCopy)
		for _, t := range tags {
			enc.AddTagRaw(t.k, t.v)
		}
		for _, f := range keepFields {
			enc.AddFieldRaw(f.key, f.val)
		}
		enc.EndLine(metricTS)
		if enc.Err() != nil {
			log.Printf("float_dedup: encode: %v", enc.Err())
			enc.ClearErr()
		} else {
			out.Write(enc.Bytes())
			out.WriteByte('\n')
		}

		tags = tags[:0]
		keepFields = keepFields[:0]
	}

	if err := dec.Err(); err != nil {
		log.Printf("float_dedup: decoder error: %v", err)
	}

	out.Flush()

	total := passed + dropped
	pct := 0.0
	if total > 0 {
		pct = 100.0 * float64(dropped) / float64(total)
	}
	log.Printf("float_dedup: done — passed=%d dropped=%d (%.1f%% reduction)",
		passed, dropped, pct)
}
