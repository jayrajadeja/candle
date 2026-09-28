package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jayrajadeja/candle/candle"
)

// fakeSource is a scripted candle.Source for handler tests.
type fakeSource struct {
	candles []candle.Candle
	err     error
	// captured args of the last call
	gotSymbol                string
	gotWidth, gotFrom, gotTo int64
}

func (f *fakeSource) Candles(symbol string, width, from, to int64) ([]candle.Candle, error) {
	f.gotSymbol, f.gotWidth, f.gotFrom, f.gotTo = symbol, width, from, to
	return f.candles, f.err
}

func do(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	rec := do(t, Handler(&fakeSource{}), "GET", "/healthz")
	if rec.Code != 200 || rec.Body.String() != "ok" {
		t.Fatalf("healthz = %d %q", rec.Code, rec.Body.String())
	}
}

func TestCandlesHappyPath(t *testing.T) {
	src := &fakeSource{candles: []candle.Candle{
		{Start: 0, Open: 10, High: 12, Low: 9, Close: 11, Volume: 5, VWAP: 10, Trades: 3, BuyVol: 3, SellVol: 2},
	}}
	rec := do(t, Handler(src), "GET", "/v1/candles?symbol=SYNTH&width=100&from=0&to=500")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body %s", rec.Code, rec.Body.String())
	}
	if src.gotSymbol != "SYNTH" || src.gotWidth != 100 || src.gotFrom != 0 || src.gotTo != 500 {
		t.Fatalf("args = %q %d %d %d", src.gotSymbol, src.gotWidth, src.gotFrom, src.gotTo)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("json: %v", err)
	}
	if got["count"].(float64) != 1 {
		t.Fatalf("count = %v", got["count"])
	}
	c0 := got["candles"].([]any)[0].(map[string]any)
	if c0["vwap"].(float64) != 10 || c0["sellVol"].(float64) != 2 {
		t.Fatalf("candle dto = %v", c0)
	}
}

func TestCandlesDefaultRange(t *testing.T) {
	src := &fakeSource{candles: []candle.Candle{}}
	do(t, Handler(src), "GET", "/v1/candles?symbol=X&width=10")
	if src.gotFrom != minInt64 || src.gotTo != maxInt64 {
		t.Fatalf("defaults = %d %d", src.gotFrom, src.gotTo)
	}
}

func TestCandlesEmptyIsArrayNotNull(t *testing.T) {
	rec := do(t, Handler(&fakeSource{candles: nil}), "GET", "/v1/candles?symbol=X&width=10")
	if rec.Code != 200 {
		t.Fatalf("code = %d", rec.Code)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("json: %v", err)
	}
	if string(raw["candles"]) != "[]" {
		t.Fatalf("candles = %s, want []", raw["candles"])
	}
}

func TestCandlesValidation(t *testing.T) {
	h := Handler(&fakeSource{})
	cases := []struct {
		name, target string
		wantCode     int
	}{
		{"missing symbol", "/v1/candles?width=10", 400},
		{"blank symbol", "/v1/candles?symbol=&width=10", 400},
		{"missing width", "/v1/candles?symbol=X", 400},
		{"zero width", "/v1/candles?symbol=X&width=0", 400},
		{"negative width", "/v1/candles?symbol=X&width=-5", 400},
		{"non-int width", "/v1/candles?symbol=X&width=abc", 400},
		{"bad from", "/v1/candles?symbol=X&width=10&from=xx", 400},
		{"bad to", "/v1/candles?symbol=X&width=10&to=xx", 400},
		{"unknown path", "/v1/nope", 404},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := do(t, h, "GET", c.target)
			if rec.Code != c.wantCode {
				t.Fatalf("%s: code = %d, want %d (body %s)", c.name, rec.Code, c.wantCode, rec.Body.String())
			}
			if rec.Code >= 400 && rec.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("%s: error not JSON: %s", c.name, rec.Header().Get("Content-Type"))
			}
		})
	}
}

func TestFromGreaterThanToIsEmpty200(t *testing.T) {
	src := &fakeSource{candles: []candle.Candle{}}
	rec := do(t, Handler(src), "GET", "/v1/candles?symbol=X&width=10&from=500&to=100")
	if rec.Code != 200 {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestNonGetIs405(t *testing.T) {
	for _, m := range []string{"POST", "PUT", "DELETE"} {
		rec := do(t, Handler(&fakeSource{}), m, "/v1/candles?symbol=X&width=10")
		if rec.Code != 405 {
			t.Fatalf("%s: code = %d, want 405", m, rec.Code)
		}
	}
}

func TestSourceErrorIs500(t *testing.T) {
	rec := do(t, Handler(&fakeSource{err: errBoom}), "GET", "/v1/candles?symbol=X&width=10")
	if rec.Code != 500 {
		t.Fatalf("code = %d, want 500", rec.Code)
	}
}

var errBoom = &boomError{}

type boomError struct{}

func (*boomError) Error() string { return "boom" }
