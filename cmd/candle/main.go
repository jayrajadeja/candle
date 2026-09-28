// Command candle reads a stream of 25-byte tick records (from a file or stdin)
// and prints OHLCV candles bucketed by logical-TS width, as a table or CSV.
//
//	lob-replay --emit | candle --width 20
//	candle --width 20 --log data/SYNTH.log
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/tick"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("candle", flag.ContinueOnError)
	fs.SetOutput(stderr)
	width := fs.Int64("width", 0, "logical-TS bucket width (required, > 0)")
	csv := fs.Bool("csv", false, "emit CSV instead of a table")
	logMode := fs.Bool("log", false, "input is a tickstore .log file (strip 8-byte header)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *width <= 0 {
		fmt.Fprintln(stderr, "candle: --width must be a positive integer")
		return 2
	}

	src, closeFn, err := openSource(fs.Arg(0), stdin)
	if err != nil {
		fmt.Fprintf(stderr, "candle: %v\n", err)
		return 1
	}
	defer closeFn()

	ticks, err := readTicks(src, *logMode)
	if err != nil {
		fmt.Fprintf(stderr, "candle: %v\n", err)
		return 1
	}

	candles, err := candle.Aggregate(ticks, *width)
	if err != nil {
		fmt.Fprintf(stderr, "candle: %v\n", err)
		return 1
	}

	if *csv {
		renderCSV(stdout, candles)
	} else {
		renderTable(stdout, candles)
	}
	return 0
}

// openSource returns the input reader: a file when path is non-empty, else stdin.
func openSource(path string, stdin io.Reader) (io.Reader, func(), error) {
	if path == "" {
		return stdin, func() {}, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { f.Close() }, nil
}

// readTicks frames the input into 25-byte records. In log mode it first strips
// and validates the 8-byte tickstore header. A trailing partial record is an error.
func readTicks(r io.Reader, logMode bool) ([]tick.Tick, error) {
	br := bufio.NewReader(r)
	if logMode {
		var h [tick.HeaderSize]byte
		if _, err := io.ReadFull(br, h[:]); err != nil {
			return nil, fmt.Errorf("reading log header: %w", err)
		}
		if err := tick.ValidateHeader(h[:]); err != nil {
			return nil, err
		}
	}
	out := make([]tick.Tick, 0)
	var buf [tick.RecordSize]byte
	for {
		_, err := io.ReadFull(br, buf[:])
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.New("truncated stream: trailing partial record")
		}
		if err != nil {
			return nil, err
		}
		tk, err := tick.Decode(buf[:])
		if err != nil {
			return nil, err
		}
		out = append(out, tk)
	}
}

func renderCSV(w io.Writer, candles []candle.Candle) {
	fmt.Fprintln(w, "start,open,high,low,close,volume,vwap,trades,buy_vol,sell_vol")
	for _, c := range candles {
		fmt.Fprintf(w, "%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
			c.Start, c.Open, c.High, c.Low, c.Close, c.Volume, c.VWAP, c.Trades, c.BuyVol, c.SellVol)
	}
}

func renderTable(w io.Writer, candles []candle.Candle) {
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "start\topen\thigh\tlow\tclose\tvolume\tvwap\ttrades\tbuyVol\tsellVol")
	for _, c := range candles {
		fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
			c.Start, c.Open, c.High, c.Low, c.Close, c.Volume, c.VWAP, c.Trades, c.BuyVol, c.SellVol)
	}
	tw.Flush()
}
