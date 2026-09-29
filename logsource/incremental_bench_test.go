package logsource

import (
	"testing"

	"github.com/jayrajadeja/candle/candle"
)

// benchmarkAppendQuery models the ingest-heavy dashboard workload: append a batch of
// ticks, then run a full-range query, repeatedly. It is parameterised over the source
// so Cache (full re-read + re-aggregate) and IncrementalCache (delta read + maintained
// candles) are measured on identical work.
func benchmarkAppendQuery(b *testing.B, newSrc func(dir string) candle.Source) {
	dir := b.TempDir()
	all := seedTicks(200000)
	writeLog(b, dir, "SYNTH", all[:2000])
	src := newSrc(dir)
	if _, err := src.Candles("SYNTH", 250, minInt64, maxInt64); err != nil { // warm
		b.Fatal(err)
	}
	next := 2000
	const batch = 100
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if next+batch > len(all) {
			b.StopTimer()
			next = 2000 // wrap without timing the file reset
			writeLog(b, dir, "SYNTH", all[:next])
			src = newSrc(dir)
			if _, err := src.Candles("SYNTH", 250, minInt64, maxInt64); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		appendRecords(b, dir, "SYNTH", all[next:next+batch])
		next += batch
		if _, err := src.Candles("SYNTH", 250, minInt64, maxInt64); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheAppendQuery(b *testing.B) {
	benchmarkAppendQuery(b, func(dir string) candle.Source { return NewCached(dir) })
}

func BenchmarkIncrementalAppendQuery(b *testing.B) {
	benchmarkAppendQuery(b, func(dir string) candle.Source { return NewIncremental(dir) })
}

// benchmarkQueryFullRange isolates the read path: a large static log queried
// full-range repeatedly. Cache re-filters and re-aggregates every tick per call;
// IncrementalCache returns a slice of its maintained candles.
func benchmarkQueryFullRange(b *testing.B, src candle.Source) {
	if _, err := src.Candles("SYNTH", 250, minInt64, maxInt64); err != nil { // warm
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := src.Candles("SYNTH", 250, minInt64, maxInt64); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCacheQueryFullRange(b *testing.B) {
	dir := b.TempDir()
	writeLog(b, dir, "SYNTH", seedTicks(200000))
	benchmarkQueryFullRange(b, NewCached(dir))
}

func BenchmarkIncrementalQueryFullRange(b *testing.B) {
	dir := b.TempDir()
	writeLog(b, dir, "SYNTH", seedTicks(200000))
	benchmarkQueryFullRange(b, NewIncremental(dir))
}
