// Pattern parsing for typed and named parameters.
//
// User-facing pattern syntax extends the uWS route grammar with optional
// type constraints attached to named parameters:
//
//	/users/:id              — bare named param (no constraint)
//	/users/:id<int>         — id must be a signed integer (regexp: -?[0-9]+)
//	/users/:id<uuid>        — id must look like a v4 UUID
//	/files/:name<alnum>     — name must be alphanumeric only
//	/blog/:slug<slug>       — kebab-case identifier
//
// The constraint annotation lives only in the gogo-facing pattern. At
// registration time the framework strips the `<type>` markers, hands uWS
// the bare pattern (uWS does not parse the angle brackets), and stashes
// the per-index constraints on a routeMeta struct. The route handler
// wrapper then validates the live param values against those constraints
// on every request — a mismatch returns 404 from inside the wrapper.

package gogo

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// paramConstraint is the per-param validator a typed-param annotation
// resolves to at registration time. Constraints are looked up by name
// in paramTypeRegistry; users register custom ones via
// RegisterParamType.
type paramConstraint struct {
	name  string // type name as written in the pattern, for error messages
	check func(string) bool
}

// routeMeta carries per-route information that the request-time wrapper
// uses to populate req.paramNames and run typed-param validation. One
// instance per registered dynamic route; static routes (Reply / string
// / []byte targets) have no meta because they cannot call Param().
type routeMeta struct {
	pattern     string
	paramNames  []string                // index → name
	constraints map[int]paramConstraint // index → constraint (sparse)
}

// parseRoutePattern walks pattern, returns the uWS-compatible pattern
// (with `<type>` markers stripped) plus a routeMeta capturing names
// and constraints. Panics on malformed `<type>` (missing '>' or
// unknown type) so misconfiguration is caught at registration, not at
// request time.
func parseRoutePattern(pattern string) (uwsPattern string, meta *routeMeta) {
	if !strings.ContainsAny(pattern, ":<") {
		// Fast path: no named params, no constraints. Skip the
		// allocation; the wrapper will see meta == nil and won't
		// install a names/constraints preamble.
		return pattern, nil
	}

	var (
		out         strings.Builder
		names       []string
		constraints map[int]paramConstraint
		paramIdx    = -1
	)
	out.Grow(len(pattern))

	i := 0
	for i < len(pattern) {
		c := pattern[i]
		if c != ':' {
			out.WriteByte(c)
			i++
			continue
		}

		// Found ':'. Read the param name up to the next '/', '<', or end.
		out.WriteByte(':')
		nameStart := i + 1
		j := nameStart
		for j < len(pattern) {
			x := pattern[j]
			if x == '/' || x == '<' {
				break
			}
			j++
		}
		name := pattern[nameStart:j]
		if name == "" {
			panic(fmt.Sprintf("gogo: route pattern %q has an empty param name (':' followed by '/' or '<')", pattern))
		}
		out.WriteString(name)
		paramIdx++
		names = append(names, name)

		// Optional <type> follows.
		if j < len(pattern) && pattern[j] == '<' {
			endAngle := strings.IndexByte(pattern[j:], '>')
			if endAngle < 0 {
				panic(fmt.Sprintf("gogo: route pattern %q has '<' without matching '>'", pattern))
			}
			typeName := pattern[j+1 : j+endAngle]
			if typeName == "" {
				panic(fmt.Sprintf("gogo: route pattern %q has an empty <type> annotation", pattern))
			}
			check := lookupParamType(typeName)
			if check == nil {
				panic(fmt.Sprintf("gogo: route pattern %q references unknown param type <%s>", pattern, typeName))
			}
			if constraints == nil {
				constraints = make(map[int]paramConstraint)
			}
			constraints[paramIdx] = paramConstraint{name: typeName, check: check}
			// Skip past '>'. Don't write the <type> chunk to uWS.
			j += endAngle + 1
		}
		i = j
	}

	uwsPattern = out.String()
	if len(names) == 0 && len(constraints) == 0 {
		return uwsPattern, nil
	}
	return uwsPattern, &routeMeta{
		pattern:     pattern,
		paramNames:  names,
		constraints: constraints,
	}
}

// paramTypeRegistry holds the built-in plus user-registered param-type
// validators. Read on every route registration (rare) and never on the
// request hot path — once parsed, the constraint sits on the routeMeta
// as a captured function value.
var paramTypeRegistry struct {
	sync.RWMutex
	m map[string]func(string) bool
}

// reInt matches a signed decimal integer. Matches Go strconv.Atoi's
// accepted syntax — no leading '+' or whitespace, optional '-' sign.
var (
	reInt   = regexp.MustCompile(`^-?[0-9]+$`)
	reUint  = regexp.MustCompile(`^[0-9]+$`)
	reUUID  = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	reAlpha = regexp.MustCompile(`^[A-Za-z]+$`)
	reAlnum = regexp.MustCompile(`^[A-Za-z0-9]+$`)
	reSlug  = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

func init() {
	paramTypeRegistry.m = map[string]func(string) bool{
		"int":   reInt.MatchString,
		"uint":  reUint.MatchString,
		"uuid":  reUUID.MatchString,
		"alpha": reAlpha.MatchString,
		"alnum": reAlnum.MatchString,
		"slug":  reSlug.MatchString,
	}
}

// RegisterParamType makes the typed-param annotation <name> available
// in route patterns app-wide. The check function returns true for
// valid values and false for invalid ones; invalid values cause the
// route wrapper to respond 404 without calling the handler.
//
// Registration is global (not per-App). Call it during process init
// — typically from an init() function or main() before any routes are
// registered. Re-registering an existing name (including the built-in
// types int/uint/uuid/alpha/alnum/slug) overwrites the prior check.
//
//	gogo.RegisterParamType("hex", func(s string) bool {
//	    for i := 0; i < len(s); i++ {
//	        c := s[i]
//	        if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
//	            return false
//	        }
//	    }
//	    return len(s) > 0
//	})
//
//	app.Get("/blobs/:digest<hex>", handler)
func RegisterParamType(name string, check func(string) bool) {
	if name == "" {
		panic("gogo: RegisterParamType requires a non-empty name")
	}
	if check == nil {
		panic("gogo: RegisterParamType requires a non-nil check function")
	}
	paramTypeRegistry.Lock()
	paramTypeRegistry.m[name] = check
	paramTypeRegistry.Unlock()
}

func lookupParamType(name string) func(string) bool {
	paramTypeRegistry.RLock()
	defer paramTypeRegistry.RUnlock()
	return paramTypeRegistry.m[name]
}
