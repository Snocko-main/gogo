package middleware

import "testing"

func TestJWTAuthParamEscapesChallengeValue(t *testing.T) {
	got := jwtAuthParam("bad \"token\"\\value\r\nnext")
	want := `bad \"token\"\\valuenext`
	if got != want {
		t.Fatalf("jwtAuthParam() = %q, want %q", got, want)
	}
}
