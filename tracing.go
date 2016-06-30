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
	"log"
	"net"
	"strconv"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"golang.org/x/net/context"

	"github.com/uber/tchannel-go/trand"
	"github.com/uber/tchannel-go/typed"
)

// ZipkinSpanFormat defines a name for OpenTracing carrier format that tracer may support.
// It is used to extract zipkin-style trace/span IDs from the OpenTracing Span, which are
// otherwise not exposed explicitly.
const ZipkinSpanFormat string = "zipkin-span-format"

// Span is an internal representation of Zipkin-compatible OpenTracing Span.
// It is used as OpenTracing inject/extract Carrier with ZipkinSpanFormat.
type Span struct {
	traceID  uint64
	parentID uint64
	spanID   uint64
	flags    byte
}

// traceRng is a thread-safe random number generator for generating trace IDs.
var traceRng = trand.NewSeeded()

func (s Span) String() string {
	return fmt.Sprintf("TraceID=%x,ParentID=%x,SpanID=%x", s.traceID, s.parentID, s.spanID)
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

func (s *Span) initRandom() {
	s.traceID = uint64(traceRng.Int63())
	s.spanID = s.traceID
	s.parentID = 0
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
	if err := span.Tracer().Inject(span, ZipkinSpanFormat, s); err != nil {
		s.initRandom()
	}
}

func (c *Connection) startOutboundSpan(ctx context.Context, serviceName, methodName string, call *OutboundCall, startTime time.Time) opentracing.Span {
	parentSpan := opentracing.SpanFromContext(ctx)
	log.Printf("parent span %+v", parentSpan)
	span := c.Tracer().StartSpanWithOptions(opentracing.StartSpanOptions{
		OperationName: serviceName + "::" + methodName,
		Parent:        parentSpan,
		StartTime:     startTime,
	})
	log.Printf("child span %+v", span)
	ext.SpanKind.Set(span, ext.SpanKindRPCClient)
	ext.PeerService.Set(span, serviceName)
	setPeerHostPort(span, c.remotePeerInfo.HostPort)
	span.SetTag("as", call.callReq.Headers[ArgScheme])
	if isTracingDisabled(ctx) {
		ext.SamplingPriority.Set(span, 0)
	}
	call.callReq.Tracing.initFromOpenTracing(span)
	return span
}

// InjectOutboundSpan serializes the tracing span from the `response` into the `headers`.
func InjectOutboundSpan(response *OutboundCallResponse, headers map[string]string) map[string]string {
	span := response.span
	if span == nil {
		return headers
	}
	if headers == nil {
		headers = make(map[string]string)
	}
	carrier := opentracing.TextMapCarrier(headers)
	if err := span.Tracer().Inject(span, opentracing.TextMap, carrier); err == nil {
		return headers
	}
	return headers
}

func (c *Connection) extractInboundSpan(callReq *callReq) opentracing.Span {
	log.Printf("incoming TChannel span: %+v", callReq.Tracing)
	span, err := c.Tracer().Join("", ZipkinSpanFormat, &callReq.Tracing)
	if span != nil {
		ext.SpanKind.Set(span, ext.SpanKindRPCServer)
		ext.PeerService.Set(span, callReq.Headers[CallerName])
		span.SetTag("as", callReq.Headers[ArgScheme])
		setPeerHostPort(span, c.remotePeerInfo.HostPort)
		log.Printf("inbound span extracted: %+v", span)
		return span
	}
	log.Printf("unable to parse span: %+v", err)
	if err != opentracing.ErrUnsupportedFormat && err != opentracing.ErrTraceNotFound {
		c.log.Error("Failed to extract Zipkin-style span: " + err.Error())
	}
	return nil
}

// TODO temporary adapter to access non-standard baggage iterator interface
type baggageIterator interface {
	ForeachBaggageItem(handler func(k, v string))
}

// ExtractInboundSpan deserializes tracing span from the incoming `headers`.
// If the response object already contains a pre-deserialized span (only for Zipkin-compatible tracers),
// then the baggage is extracted from the headers and added to that span.
func ExtractInboundSpan(ctx context.Context, call *InboundCall, headers map[string]string, tracer opentracing.Tracer) context.Context {
	var span = call.Response().span
	operationName := call.ServiceName() + "::" + call.MethodString()
	if span != nil {
		log.Printf("found span %+v\n", span)
		// copy baggage from headers
		if headers != nil {
			carrier := opentracing.TextMapCarrier(headers)
			// TODO right now we're creating a second span. Once PR #82 lands in opentracing-go,
			// we'll only extract the SpanContext, which will directly support ForeachBaggageItem
			if sp, _ := tracer.Join(operationName, opentracing.TextMap, carrier); sp != nil {
				if bsp, ok := sp.(baggageIterator); ok {
					log.Printf("copying baggage from %+v\n", bsp)
					bsp.ForeachBaggageItem(func(k, v string) {
						span.SetBaggageItem(k, v)
					})
				} else {
					// TODO after PR #82, we should never reach this point
				}
			}
		}
	} else {
		if headers != nil {
			carrier := opentracing.TextMapCarrier(headers)
			if sp, _ := tracer.Join(operationName, opentracing.TextMap, carrier); sp != nil {
				span = sp
			}
		}
		if span == nil {
			span = tracer.StartSpan(operationName)
		}
		ext.SpanKind.Set(span, ext.SpanKindRPCServer)
		ext.PeerService.Set(span, call.CallerName())
		span.SetTag("as", "json")
		setPeerHostPort(span, call.RemotePeer().HostPort)
	}
	return opentracing.ContextWithSpan(ctx, span)
}

func setPeerHostPort(span opentracing.Span, hostPort string) {
	if host, port, err := net.SplitHostPort(hostPort); err == nil {
		ext.PeerHostname.Set(span, host)
		if p, err := strconv.Atoi(port); err == nil {
			ext.PeerPort.Set(span, uint16(p))
		}
	} else {
		span.SetTag(string(ext.PeerHostIPv4), host)
	}
}
