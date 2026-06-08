//go:build cgo && gogo

package gogo

import (
	"testing"
	"time"
)

func TestNativeIntClampHelpers(t *testing.T) {
	if got := clampNonNegativeInt(-1); got != 0 {
		t.Fatalf("clampNonNegativeInt(-1) = %d, want 0", got)
	}
	if got := int(cIntFromNonNegative(-1)); got != 0 {
		t.Fatalf("cIntFromNonNegative(-1) = %d, want 0", got)
	}
	if got := int(cIntFromInt(maxInt32)); got != maxInt32 {
		t.Fatalf("cIntFromInt(maxInt32) = %d, want %d", got, maxInt32)
	}
	if got := int(cIntFromInt(minInt32)); got != minInt32 {
		t.Fatalf("cIntFromInt(minInt32) = %d, want %d", got, minInt32)
	}

	if maxGoInt > maxInt32 {
		if got := int(cIntFromInt(int(maxInt32) + 1)); got != maxInt32 {
			t.Fatalf("cIntFromInt(maxInt32+1) = %d, want %d", got, maxInt32)
		}
		if got := int(cIntFromInt(int(minInt32) - 1)); got != minInt32 {
			t.Fatalf("cIntFromInt(minInt32-1) = %d, want %d", got, minInt32)
		}
	}
}

func TestNativeUnsignedWidthClampHelpers(t *testing.T) {
	if got := uint64(cUint32SizeFromPositive(0, 4096)); got != 4096 {
		t.Fatalf("cUint32SizeFromPositive(0, 4096) = %d, want 4096", got)
	}
	if got := uint64(cUint32SizeFromPositive(-1, 4096)); got != 4096 {
		t.Fatalf("cUint32SizeFromPositive(-1, 4096) = %d, want 4096", got)
	}
	if uint64(maxGoInt) > maxUint32 {
		if got := uint64(cUint32SizeFromPositive(int(maxUint32)+1, 1)); got != maxUint32 {
			t.Fatalf("cUint32SizeFromPositive(maxUint32+1, 1) = %d, want %d", got, uint64(maxUint32))
		}
	}

	if got := int(cUint16IntFromDurationSeconds(0, 120)); got != 120 {
		t.Fatalf("cUint16IntFromDurationSeconds(0, 120) = %d, want 120", got)
	}
	if got := int(cUint16IntFromDurationSeconds(70000*time.Second, 120)); got != maxUint16 {
		t.Fatalf("cUint16IntFromDurationSeconds(70000s, 120) = %d, want %d", got, maxUint16)
	}
}

func TestNativeBoundedUint32Len(t *testing.T) {
	if got := boundedUint32Len(7, 64); got != 7 {
		t.Fatalf("boundedUint32Len(7, 64) = %d, want 7", got)
	}
	if got := boundedUint32Len(99, 8); got != 8 {
		t.Fatalf("boundedUint32Len(99, 8) = %d, want 8", got)
	}
	if got := boundedUint32Len(uint32(maxUint32), 8192); got != 8192 {
		t.Fatalf("boundedUint32Len(maxUint32, 8192) = %d, want 8192", got)
	}
}
