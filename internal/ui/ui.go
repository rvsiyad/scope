// Package ui is the demo layer: a small dashboard served straight out of
// the collector binary. The files are embedded at build time so the whole
// system stays one artifact per process — no node toolchain, no asset
// pipeline, no separate web server to deploy or keep in sync. The UI is
// deliberately plain JS talking to the same query API any other client
// would use (/v1/query_range, /v1/traces); it gets no private endpoints,
// which keeps the API honest — if the dashboard can render it, so can a
// curl-ing human.
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var static embed.FS

// Handler serves the dashboard's static files. Mount it under a prefix
// (the collector uses /ui/) with http.StripPrefix; index.html answers the
// bare prefix by http.FileServer's usual index rule.
func Handler() http.Handler {
	sub, err := fs.Sub(static, "static")
	if err != nil {
		// The embed is compiled in; a missing subdirectory is a build
		// defect, not a runtime condition.
		panic(err)
	}
	return http.FileServerFS(sub)
}
