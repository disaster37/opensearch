package opentelemetry

import (
	"context"
	"net/url"
	"runtime"
	"sync"

	"github.com/go-resty/resty/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type contextKey struct{}

// spanHolder wraps an OTel span with a sync.Once guard so that a GC finalizer
// can close the span if resty's OnAfterResponse is never invoked — which
// happens on transport-level failures (DNS error, connection refused, TLS
// handshake failure, context cancellation before a response is received).
// Without this safety net a leaked span accumulates in the exporter buffer
// for every failed HTTP round-trip.
//
// The sync.Once makes endSpan safe for concurrent invocation from the
// response goroutine and the runtime's finalizer goroutine (which have no
// ordering guarantee): the span is ended exactly once regardless of which
// caller wins the race.
type spanHolder struct {
	span trace.Span
	once sync.Once
}

// endSpan closes the span exactly once, regardless of how many times it is
// called (from the response middleware, from the GC finalizer, etc.). It is
// safe for concurrent use.
func (h *spanHolder) endSpan() {
	if h == nil {
		return
	}
	h.once.Do(func() { h.span.End() })
}

// Middleware returns a pair of resty middlewares that wrap each HTTP request
// in an OpenTelemetry span.
//
//   - The request middleware (before) starts a span named "opensearch.request"
//     and records http.url (redacted) and http.method as span attributes. The
//     span is stored in the request context so the response middleware can
//     retrieve it.
//
//   - The response middleware (after) ends the span and records the HTTP
//     status code. It also marks the response as an error when the status
//     code is >= 400.
//
// Span lifecycle on network failures: if the HTTP exchange fails before a
// response is received (e.g. dial error, DNS lookup failure), OnAfterResponse
// is not invoked and the span would normally be leaked. To guard against this,
// each span is protected by a GC finalizer: it is automatically ended when
// the corresponding resty Request is garbage-collected.
func Middleware(tracerName string) (resty.RequestMiddleware, resty.ResponseMiddleware) {
	if tracerName == "" {
		tracerName = "github.com/disaster37/opensearch/v4"
	}

	before := func(_ *resty.Client, req *resty.Request) error {
		ctx, span := otel.Tracer(tracerName).Start(req.Context(), "opensearch.request")

		holder := &spanHolder{span: span}
		// The http.Request pointer is used as the finalization target. Once
		// the request is garbage-collected, the holder finalizer ends the
		// span if the response middleware never fired.
		runtime.SetFinalizer(req, func(r *resty.Request) {
			holder.endSpan()
		})

		req.SetContext(context.WithValue(ctx, contextKey{}, holder))

		span.SetAttributes(
			attribute.String("http.url", redactURL(req.URL)),
			attribute.String("http.method", req.Method),
		)
		return nil
	}

	after := func(_ *resty.Client, resp *resty.Response) error {
		holder, ok := resp.Request.Context().Value(contextKey{}).(*spanHolder)
		if !ok || holder == nil {
			return nil
		}
		// On the happy path the response middleware has fired, so the span
		// will be ended below by the deferred endSpan call. Clear the
		// finalizer we set in `before` to avoid the two-pass GC penalty
		// Go imposes on objects with finalizers: detaching it here lets
		// the request be freed in a single collection cycle and prevents
		// the finalizer goroutine from needlessly holding the span (and
		// its attribute buffer) alive across GC cycles.
		runtime.SetFinalizer(resp.Request, nil)
		defer holder.endSpan()

		holder.span.SetAttributes(attribute.Int64("http.status_code", int64(resp.StatusCode())))

		if resp.IsError() {
			holder.span.SetStatus(codes.Error, resp.Status())
		}
		return nil
	}

	return before, after
}

// redactURL strips any embedded userinfo (username and/or password) from a
// raw URL. This is more aggressive than net/url.Redacted, which preserves
// the username when no password is present — we remove the entire userinfo
// section because tokens, API keys, and short-lived credentials are often
// placed in the username slot alone (e.g. "https://api-key-12345@host"),
// and we must not leak them into exported OTel span attributes per the OTel
// HTTP semantic conventions. If the URL cannot be parsed it is returned
// unchanged.
func redactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	u.User = nil
	return u.String()
}
