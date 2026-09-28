package tick

import (
	"encoding/hex"
	"errors"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// Golden record bytes shared byte-for-byte with lob's --emit and tickstore's
// tick package. If any project changes the wire layout, this test fails.
func TestDecodeGolden(t *testing.T) {
	cases := []struct {
		hexStr string
		want   Tick
	}{
		{"01000000000000006400000000000000050000000000000000", Tick{TS: 1, Price: 100, Qty: 5, Side: Buy}},
		{"02000000000000006500000000000000020000000000000001", Tick{TS: 2, Price: 101, Qty: 2, Side: Sell}},
	}
	for _, c := range cases {
		got, err := Decode(mustHex(t, c.hexStr))
		if err != nil {
			t.Fatalf("Decode(%s) error: %v", c.hexStr, err)
		}
		if got != c.want {
			t.Errorf("Decode(%s) = %+v, want %+v", c.hexStr, got, c.want)
		}
	}
}

func TestDecodeBadSize(t *testing.T) {
	if _, err := Decode(make([]byte, 24)); !errors.Is(err, ErrBadSize) {
		t.Fatalf("Decode(24 bytes) err = %v, want ErrBadSize", err)
	}
	if _, err := Decode(make([]byte, 26)); !errors.Is(err, ErrBadSize) {
		t.Fatalf("Decode(26 bytes) err = %v, want ErrBadSize", err)
	}
}

func TestValidateHeader(t *testing.T) {
	good := append([]byte("TCKLOG"), 0x01, 0x00)
	if err := ValidateHeader(good); err != nil {
		t.Fatalf("ValidateHeader(good) = %v, want nil", err)
	}
	badMagic := append([]byte("NOPELG"), 0x01, 0x00)
	if err := ValidateHeader(badMagic); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("bad magic err = %v, want ErrBadMagic", err)
	}
	badVer := append([]byte("TCKLOG"), 0x02, 0x00)
	if err := ValidateHeader(badVer); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("bad version err = %v, want ErrBadVersion", err)
	}
	if err := ValidateHeader(make([]byte, 7)); !errors.Is(err, ErrBadSize) {
		t.Fatalf("short header err = %v, want ErrBadSize", err)
	}
}

func TestSideString(t *testing.T) {
	if Buy.String() != "buy" || Sell.String() != "sell" {
		t.Fatalf("Side.String: buy=%q sell=%q", Buy.String(), Sell.String())
	}
}
