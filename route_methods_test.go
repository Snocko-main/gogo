package gogo

import (
	"reflect"
	"testing"
)

func TestAllowedMethodsMatchesDynamicWildcardAndTypedRoutes(t *testing.T) {
	var app App

	dynamicPattern, dynamicMeta := parseRoutePattern("/api/:section")
	app.trackRouteMethod("get", dynamicPattern, dynamicMeta)
	app.trackRouteMethod("post", dynamicPattern, dynamicMeta)

	wildcardPattern, wildcardMeta := parseRoutePattern("/assets/*")
	app.trackRouteMethod("delete", wildcardPattern, wildcardMeta)

	typedPattern, typedMeta := parseRoutePattern("/users/:id<int>")
	app.trackRouteMethod("patch", typedPattern, typedMeta)

	tests := []struct {
		name string
		path string
		want []string
	}{
		{
			name: "dynamic",
			path: "/api/admin",
			want: []string{"GET", "POST"},
		},
		{
			name: "dynamic empty param misses",
			path: "/api/",
		},
		{
			name: "wildcard descendant",
			path: "/assets/css/site.css",
			want: []string{"DELETE"},
		},
		{
			name: "wildcard trailing slash",
			path: "/assets/",
			want: []string{"DELETE"},
		},
		{
			name: "wildcard bare prefix misses",
			path: "/assets",
		},
		{
			name: "typed valid",
			path: "/users/42",
			want: []string{"PATCH"},
		},
		{
			name: "typed invalid misses",
			path: "/users/alice",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := app.AllowedMethods(tc.path); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("AllowedMethods(%q) = %#v, want %#v", tc.path, got, tc.want)
			}
		})
	}
}

func TestRouteMethodWildcardMustBeTerminal(t *testing.T) {
	entry := newRouteMethodEntry("get", "/dead/*/tail", nil)
	if entry.matches("/dead/anything/tail") {
		t.Fatal("non-terminal wildcard route matched, but uWS only executes handlers on the wildcard node itself")
	}
}
