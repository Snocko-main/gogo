package middleware

import "testing"

func TestHelmetRejectsInvalidHeaderValuesAtConstruction(t *testing.T) {
	cases := []HelmetOptions{
		{HSTS: "max-age=1\r\nX-Evil: yes"},
		{ContentSecurityPolicy: "default-src 'self'\nscript-src *"},
		{FrameOptions: "DENY\x00"},
	}
	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("Helmet did not panic")
				}
			}()
			_ = Helmet(tc)
		})
	}
}

func TestHelmetAllowsOmittedInvalidSentinelValues(t *testing.T) {
	_ = Helmet(HelmetOptions{
		HSTS:         "off",
		FrameOptions: "omit",
	})
}
