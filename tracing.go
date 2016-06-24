// Copyright (c) 2015 Uber Technologies, Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tchannel

import (
	"fmt"

	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/uber/tchannel-go/typed"
)

// ZipkinSpanFormat defines a name for OpenTracing carrier format that tracer may support.
// It is used to extract zipkin-style trace/span IDs from the OpenTracing Span, which are
// otherwise not exposed explicitly.
const ZipkinSpanFormat string = "zipkin-span-format"

// Span is an internal representation of Zipkin-compatible OpenTracing Span.
// It can be used as OpenTracing inject/extract Carrier.
//
// TODO extend span serialization to support non-Zipkin-like tracing systems.
type Span struct {
	traceID  uint64
	parentID uint64
	spanID   uint64
	flags    byte
}

func (s Span) String() string {
	return fmt.Sprintf("TraceID=%d,ParentID=%d,SpanID=%d", s.traceID, s.parentID, s.spanID)
}

func (s *Span) read(r *typed.ReadBuffer) error {
	s.spanID = r.ReadUint64()
	s.parentID = r.ReadUint64()
	s.traceID = r.ReadUint64()
	s.flags = r.ReadSingleByte()
	return r.Err()
}

func (s *Span) write(w *typed.WriteBuffer) error {
	w.WriteUint64(s.spanID)
	w.WriteUint64(s.parentID)
	w.WriteUint64(s.traceID)
	w.WriteSingleByte(s.flags)
	return w.Err()
}

// TraceID returns the trace id for the entire call graph of requests. Established
// at the outermost edge service and propagated through all calls
func (s Span) TraceID() uint64 { return s.traceID }

// ParentID returns the id of the parent span in this call graph
func (s Span) ParentID() uint64 { return s.parentID }

// SpanID returns the id of this specific RPC
func (s Span) SpanID() uint64 { return s.spanID }

// Flags returns flags bitmap. Interpretation of the bits is up to the tracing system.
func (s Span) Flags() byte { return s.flags }

// SetTraceID sets traceID
func (s *Span) SetTraceID(traceID uint64) { s.traceID = traceID }

// SetSpanID sets spanID
func (s *Span) SetSpanID(spanID uint64) { s.spanID = spanID }

// SetParentID sets parentID
func (s *Span) SetParentID(parentID uint64) { s.parentID = parentID }

// SetFlags sets flags
func (s *Span) SetFlags(flags byte) { s.flags = flags }

// CurrentSpan extracts OpenTracing Span from the Context, and if found tries to
// extract zipkin-style trace/span IDs from it using ZipkinSpanFormat carrier.
func CurrentSpan(ctx context.Context) *Span {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		span := &Span{}
		span.initFromOpenTracing(sp)
		return span
	}
	return nil
}

// initFromOpenTracing initializes Span fields from an OpenTracing Span,
// assuming the tracing implementation supports Zipkin-style span IDs.
func (s *Span) initFromOpenTracing(span opentracing.Span) {
	span.Tracer().Inject(span, ZipkinSpanFormat, s)
}
