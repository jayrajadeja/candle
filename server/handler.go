// Package server exposes the candle aggregation API over HTTP as JSON. It is
// transport-only: it binds no socket, never calls os.Exit, and never reads argv,
// so it is fully testable with net/http/httptest. Process concerns (listening,
// signals, shutdown) live in cmd/candle.
package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/jayrajadeja/candle/candle"
)

const (
	minInt64 = -1 << 63
	maxInt64 = 1<<63 - 1
)

// candleDTO is the JSON shape of one candle on the wire.
type candleDTO struct {
	Start   int64  `json:"start"`
	Open    int64  `json:"open"`
	High    int64  `json:"high"`
	Low     int64  `json:"low"`
	Close   int64  `json:"close"`
	Volume  uint64 `json:"volume"`
	VWAP    int64  `json:"vwap"`
	Trades  int    `json:"trades"`
	BuyVol  uint64 `json:"buyVol"`
	SellVol uint64 `json:"sellVol"`
}

type candlesResponse struct {
	Symbol  string      `json:"symbol"`
	Width   int64       `json:"width"`
	From    int64       `json:"from"`
	To      int64       `json:"to"`
	Count   int         `json:"count"`
	Candles []candleDTO `json:"candles"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// candleToDTO maps one candle to its wire DTO.
func candleToDTO(c candle.Candle) candleDTO {
	return candleDTO{
		Start: c.Start, Open: c.Open, High: c.High, Low: c.Low, Close: c.Close,
		Volume: c.Volume, VWAP: c.VWAP, Trades: c.Trades, BuyVol: c.BuyVol, SellVol: c.SellVol,
	}
}

// toDTOs maps candles to wire DTOs, always returning a non-nil slice so the JSON
// is an empty array `[]` rather than `null`.
func toDTOs(cs []candle.Candle) []candleDTO {
	out := make([]candleDTO, 0, len(cs))
	for _, c := range cs {
		out = append(out, candleToDTO(c))
	}
	return out
}

// Handler returns an http.Handler serving the candle read API from src.
func Handler(src candle.Source) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/v1/candles", func(w http.ResponseWriter, r *http.Request) {
		handleCandles(w, r, src)
	})
	mux.Handle("/v1/stream", newStreamHandler(src, defaultStreamInterval))
	// Catch-all: any other path is a JSON 404.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, "not found")
	})
	return mux
}

func handleCandles(w http.ResponseWriter, r *http.Request, src candle.Source) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()

	symbol := q.Get("symbol")
	if symbol == "" {
		writeErr(w, http.StatusBadRequest, "missing required param: symbol")
		return
	}
	width, err := parseInt64Required(q, "width")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if width <= 0 {
		writeErr(w, http.StatusBadRequest, "width must be a positive integer")
		return
	}
	from, err := parseInt64Default(q, "from", minInt64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	to, err := parseInt64Default(q, "to", maxInt64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	cs, err := src.Candles(symbol, width, from, to)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	dtos := toDTOs(cs)
	writeJSON(w, http.StatusOK, candlesResponse{
		Symbol: symbol, Width: width, From: from, To: to, Count: len(dtos), Candles: dtos,
	})
}

// parseInt64Required parses a required int64 query param.
func parseInt64Required(q url.Values, name string) (int64, error) {
	raw := q.Get(name)
	if raw == "" {
		return 0, &paramError{"missing required param: " + name}
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, &paramError{"invalid " + name + ": not an integer"}
	}
	return v, nil
}

// parseInt64Default parses an optional int64 query param, returning def if absent.
func parseInt64Default(q url.Values, name string, def int64) (int64, error) {
	raw := q.Get(name)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, &paramError{"invalid " + name + ": not an integer"}
	}
	return v, nil
}

type paramError struct{ msg string }

func (e *paramError) Error() string { return e.msg }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
