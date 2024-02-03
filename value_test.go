package pixidb

import (
	"math"
	"testing"
)

func FuzzInt8Ctor(f *testing.F) {
	f.Add(int8(0))
	f.Add(int8(-1))
	f.Add(int8(1))
	f.Add(int8(127))
	f.Fuzz(func(t *testing.T, val int8) {
		enc := NewInt8Value(val)
		dec := enc.AsInt8()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzUint8Ctor(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Add(uint8(127))
	f.Fuzz(func(t *testing.T, val uint8) {
		enc := NewUint8Value(val)
		dec := enc.AsUint8()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzInt16Ctor(f *testing.F) {
	f.Add(int16(0))
	f.Add(int16(-1))
	f.Add(int16(1))
	f.Add(int16(127))
	f.Add(int16(20000))
	f.Add(int16(-20000))
	f.Fuzz(func(t *testing.T, val int16) {
		enc := NewInt16Value(val)
		dec := enc.AsInt16()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzUint16Ctor(f *testing.F) {
	f.Add(uint16(0))
	f.Add(uint16(1))
	f.Add(uint16(127))
	f.Add(uint16(20000))
	f.Fuzz(func(t *testing.T, val uint16) {
		enc := NewUint16Value(val)
		dec := enc.AsUint16()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzInt32Ctor(f *testing.F) {
	f.Add(int32(0))
	f.Add(int32(-1))
	f.Add(int32(1))
	f.Add(int32(127))
	f.Add(int32(20000))
	f.Add(int32(-20000))
	f.Fuzz(func(t *testing.T, val int32) {
		enc := NewInt32Value(val)
		dec := enc.AsInt32()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzUint32Ctor(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(127))
	f.Add(uint32(20000))
	f.Fuzz(func(t *testing.T, val uint32) {
		enc := NewUint32Value(val)
		dec := enc.AsUint32()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzInt64Ctor(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(-1))
	f.Add(int64(1))
	f.Add(int64(127))
	f.Add(int64(20000))
	f.Add(int64(-20000))
	f.Fuzz(func(t *testing.T, val int64) {
		enc := NewInt64Value(val)
		dec := enc.AsInt64()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzUint64Ctor(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(127))
	f.Add(uint64(20000))
	f.Fuzz(func(t *testing.T, val uint64) {
		enc := NewUint64Value(val)
		dec := enc.AsUint64()
		if val != dec {
			t.Errorf("expected %d after encode/decode, got %d", val, dec)
		}
	})
}

func FuzzFloat32Ctor(f *testing.F) {
	f.Add(float32(0.0))
	f.Add(float32(1.0))
	f.Add(float32(math.Pi))
	f.Add(float32(-math.Pi))
	f.Fuzz(func(t *testing.T, val float32) {
		enc := NewFloat32Value(val)
		dec := enc.AsFloat32()
		if val != dec {
			t.Errorf("expecte %e after encode/decode, got %e", val, dec)
		}
	})
}

func FuzzFloat64Ctor(f *testing.F) {
	f.Add(float64(0.0))
	f.Add(float64(1.0))
	f.Add(float64(math.Pi))
	f.Add(float64(-math.Pi))
	f.Fuzz(func(t *testing.T, val float64) {
		enc := NewFloat64Value(val)
		dec := enc.AsFloat64()
		if val != dec {
			t.Errorf("expecte %e after encode/decode, got %e", val, dec)
		}
	})
}
