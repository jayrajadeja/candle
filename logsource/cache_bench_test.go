package logsource

import "testing"

func BenchmarkCacheCandles(b *testing.B) {
	dir := b.TempDir()
	writeLog(b, dir, "SYNTH", seedTicks(5000))
	c := NewCached(dir)
	if _, err := c.Candles("SYNTH", 250, minTS, maxTS); err != nil { // warm the cache
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Candles("SYNTH", 250, minTS, maxTS); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLogSourceCandles(b *testing.B) {
	dir := b.TempDir()
	writeLog(b, dir, "SYNTH", seedTicks(5000))
	ls := New(dir)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ls.Candles("SYNTH", 250, minTS, maxTS); err != nil {
			b.Fatal(err)
		}
	}
}
