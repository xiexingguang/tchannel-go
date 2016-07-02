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

package tchannel_test

import (
	json_encoding "encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/json"
	"github.com/uber/tchannel-go/raw"
	"github.com/uber/tchannel-go/testutils"
	"github.com/uber/tchannel-go/thrift"
	gen "github.com/uber/tchannel-go/thrift/gen-go/test"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"golang.org/x/net/context"
)

// TracingRequest tests tracing capabilities in a given server.
type TracingRequest struct {
	// ForwardCount tells the server how many times to forward this request to itself recursively
	ForwardCount int
}

func (r *TracingRequest) fromRaw(args *raw.Args) *TracingRequest {
	r.ForwardCount = int(args.Arg3[0])
	return r
}

func (r *TracingRequest) fromThrift(req *gen.Data) *TracingRequest {
	r.ForwardCount = int(req.I3)
	return r
}

func (r *TracingRequest) toRaw() []byte {
	return []byte{byte(r.ForwardCount)}
}

func (r *TracingRequest) toThrift() *gen.Data {
	return &gen.Data{I3: int32(r.ForwardCount)}
}

// TracingResponse captures the trace info observed in the server and its downstream calls
type TracingResponse struct {
	TraceID        uint64
	SpanID         uint64
	ParentID       uint64
	TracingEnabled bool
	Child          *TracingResponse
	Luggage        string
}

func (r *TracingResponse) fromJSON(t *testing.T, data []byte) *TracingResponse {
	err := json_encoding.Unmarshal(data, r)
	require.NoError(t, err)
	return r
}

func (r *TracingResponse) fromRaw(t *testing.T, arg3 []byte) *TracingResponse {
	err := json_encoding.Unmarshal(arg3, r)
	require.NoError(t, err)
	return r
}

func (r *TracingResponse) fromThrift(t *testing.T, res *gen.Data) *TracingResponse {
	err := json_encoding.Unmarshal([]byte(res.S2), r)
	require.NoError(t, err)
	return r
}

func (r *TracingResponse) toJSON(t *testing.T) []byte {
	jsonBytes, err := json_encoding.Marshal(r)
	require.NoError(t, err)
	return jsonBytes
}

func (r *TracingResponse) toRaw(t *testing.T) *raw.Res {
	jsonBytes := r.toJSON(t)
	return &raw.Res{Arg3: jsonBytes}
}

func (r *TracingResponse) toThrift(t *testing.T) *gen.Data {
	jsonBytes := r.toJSON(t)
	return &gen.Data{S2: string(jsonBytes)}
}

func (r *TracingResponse) observeSpan(ctx context.Context) *TracingResponse {
	if span := opentracing.SpanFromContext(ctx); span != nil {
		if basicSpan, ok := span.(basictracer.Span); ok {
			r.TraceID = basicSpan.Context().TraceID
			r.SpanID = basicSpan.Context().SpanID
			r.ParentID = basicSpan.Context().ParentSpanID
			r.TracingEnabled = basicSpan.Context().Sampled
		} else if span := CurrentSpan(ctx); span != nil {
			r.TraceID = span.TraceID()
			r.SpanID = span.SpanID()
			r.ParentID = span.ParentID()
			r.TracingEnabled = span.Flags()&1 == 1
		}
		r.Luggage = span.BaggageItem(baggageKey)
	}
	return r
}

const (
	baggageKey   = "luggage"
	baggageValue = "suitcase"
)

type traceHandler struct {
	ch           *Channel
	t            *testing.T
	thriftClient gen.TChanSimpleService
}

// handleCall is used by handlers from different encodings as the main business logic
func (h *traceHandler) handleCall(
	ctx context.Context,
	req *TracingRequest,
	downstream func(ctx context.Context, req *TracingRequest) *TracingResponse,
) (*TracingResponse, error) {
	var childResp *TracingResponse
	if req.ForwardCount > 0 {
		downstreamReq := &TracingRequest{ForwardCount: req.ForwardCount - 1}
		childResp = downstream(ctx, downstreamReq)
	}

	resp := &TracingResponse{Child: childResp}
	resp.observeSpan(ctx)

	return resp, nil
}

// RawHandler tests tracing over Raw encoding
type RawHandler struct {
	traceHandler
}

func (h *RawHandler) Handle(ctx context.Context, args *raw.Args) (*raw.Res, error) {
	req := new(TracingRequest).fromRaw(args)
	res, err := h.handleCall(ctx, req,
		func(ctx context.Context, req *TracingRequest) *TracingResponse {
			_, arg3, _, err := raw.Call(ctx, h.ch, h.ch.PeerInfo().HostPort,
				h.ch.PeerInfo().ServiceName, "rawcall", nil, req.toRaw())
			require.NoError(h.t, err)
			return new(TracingResponse).fromRaw(h.t, arg3)
		})
	require.NoError(h.t, err)
	return res.toRaw(h.t), nil
}

func (h *RawHandler) OnError(ctx context.Context, err error) { h.t.Errorf("onError %v", err) }

// JSONHandler tests tracing over JSON encoding
type JSONHandler struct {
	traceHandler
}

func (h *JSONHandler) callJSON(ctx json.Context, req *TracingRequest) (*TracingResponse, error) {
	return h.handleCall(ctx, req, func(ctx context.Context, req *TracingRequest) *TracingResponse {
		jctx := ctx.(json.Context)
		sc := h.ch.Peers().GetOrAdd(h.ch.PeerInfo().HostPort)
		childResp := new(TracingResponse)
		require.NoError(h.t, json.CallPeer(jctx, sc, h.ch.PeerInfo().ServiceName, "call", req, childResp))
		return childResp
	})
}

func (h *JSONHandler) onError(ctx context.Context, err error) { h.t.Errorf("onError %v", err) }

// ThriftHandler tests tracing over Thrift encoding
type ThriftHandler struct {
	traceHandler
}

func (h *ThriftHandler) Call(ctx thrift.Context, arg *gen.Data) (*gen.Data, error) {
	req := new(TracingRequest).fromThrift(arg)
	res, err := h.handleCall(ctx, req, func(ctx context.Context, req *TracingRequest) *TracingResponse {
		tctx := ctx.(thrift.Context)
		res, err := h.thriftClient.Call(tctx, req.toThrift())
		require.NoError(h.t, err)
		return new(TracingResponse).fromThrift(h.t, res)
	})
	require.NoError(h.t, err)
	return res.toThrift(h.t), nil
}

func (h *ThriftHandler) Simple(ctx thrift.Context) error {
	return nil
}

type TracerType string

const (
	Noop   TracerType = "NOOP"
	Basic  TracerType = "BASIC"
	Jaeger TracerType = "JAEGER"
)

type tracerChoice struct {
	tracerType       TracerType
	tracer           opentracing.Tracer
	spansRecorded    func() int
	resetSpans       func()
	isFake           bool
	zipkinCompatible bool
}

type basicTracerLoggingRecorder struct {
	recorder basictracer.SpanRecorder
}

func (r *basicTracerLoggingRecorder) RecordSpan(span basictracer.RawSpan) {
	// To actually enable logging, uncomment below
	// log.Printf("Basic tracer recording span %+v", span)
	r.recorder.RecordSpan(span)
}

func TestTracingPropagation(t *testing.T) {
	jaegerReporter := jaeger.NewInMemoryReporter()
	jaegerTracer, jaegerCloser := jaeger.NewTracer(testutils.DefaultServerName,
		jaeger.NewConstSampler(true),
		jaegerReporter)
	// To enable logging, use composite reporter:
	// jaeger.NewCompositeReporter(jaegerReporter, jaeger.NewLoggingReporter(jaeger.StdLogger)))
	defer jaegerCloser.Close()
	jaeger := tracerChoice{
		tracerType:       Jaeger,
		tracer:           jaegerTracer,
		spansRecorded:    func() int { return len(jaegerReporter.GetSpans()) },
		resetSpans:       func() { jaegerReporter.Reset() },
		zipkinCompatible: true}

	basicRecorder := basictracer.NewInMemoryRecorder()
	basicTracer := basictracer.NewWithOptions(basictracer.Options{
		ShouldSample: func(traceID uint64) bool { return true },
		Recorder:     &basicTracerLoggingRecorder{basicRecorder},
	})
	basic := tracerChoice{
		tracerType:    Basic,
		tracer:        basicTracer,
		spansRecorded: func() int { return len(basicRecorder.GetSampledSpans()) },
		resetSpans:    func() { basicRecorder.Reset() },
	}

	// When tracer is not specified, opentracing.GlobalTracer() is used
	noop := tracerChoice{
		tracerType:    Noop,
		spansRecorded: func() int { return 0 },
		resetSpans:    func() {},
		isFake:        true}

	tracers := []tracerChoice{noop, basic, jaeger}
	for _, tracer := range tracers {
		testTracingPropagationWithTracer(t, tracer)
	}
}

type propagationTest struct {
	forwardCount      int
	tracingDisabled   bool
	format            Format
	expectedBaggage   string
	expectedSpanCount int
}

func testTracingPropagationWithTracer(t *testing.T, tracer tracerChoice) {
	opts := &testutils.ChannelOpts{
		ChannelOptions: ChannelOptions{Tracer: tracer.tracer},
		DisableRelay:   true,
	}
	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		opts := &thrift.ClientOptions{HostPort: ch.PeerInfo().HostPort}
		thriftClient := thrift.NewClient(ch, ch.PeerInfo().ServiceName, opts)
		simpleClient := gen.NewTChanSimpleServiceClient(thriftClient)

		handler := &traceHandler{t: t, ch: ch, thriftClient: simpleClient}

		// Register Raw handler
		rawHandler := &RawHandler{*handler}
		ch.Register(raw.Wrap(rawHandler), "rawcall")

		// Register JSON handler
		jsonHandler := &JSONHandler{*handler}
		json.Register(ch, json.Handlers{"call": jsonHandler.callJSON}, jsonHandler.onError)

		// Register Thrift handler
		server := thrift.NewServer(ch)
		thriftHandler := &ThriftHandler{*handler}
		server.Register(gen.NewTChanSimpleServiceServer(thriftHandler))

		type SpanCountMapping map[TracerType]int
		defaultTracingEnabledCounts := SpanCountMapping{Noop: 0, Basic: 6, Jaeger: 6}
		defaultTracingDisabledCounts := SpanCountMapping{Noop: 0, Basic: 0, Jaeger: 0}
		tests := []struct {
			forwardCount                     int
			format                           Format
			expectedBaggage                  string
			expectedSpanCountTracingEnabled  SpanCountMapping
			expectedSpanCountTracingDisabled SpanCountMapping
		}{
			// Raw encoding does not support application headers, thus no baggage can be propagated
			{2, Raw, "",
				map[TracerType]int{
					Noop: 0,
					// Since Raw encoding does not propagate generic traces, we record 3 spans
					// for outbound calls, but none for inbound calls.
					Basic: 3,
					// Since Jaeger is Zipkin-compatible, it is able to decode the trace
					// even from the Raw encoding.
					Jaeger: 6},
				map[TracerType]int{
					Noop: 0,
					// Since Raw encoding does not propagate generic traces, the tracingDisable
					// only affects the first outbound span (it's not sampled), but the other
					// two outbound spans are still sampled and recorded.
					Basic:  2,
					Jaeger: 0}},
			// In the rest of the tests both generic and Zipkin-compatible tracers are able
			// to propagate full trace information, so the number of sampled spans is:
			// (1 client + 1 server) * (3 hops) = 6
			{2, JSON, baggageValue, defaultTracingEnabledCounts, defaultTracingDisabledCounts},
			{2, Thrift, baggageValue, defaultTracingEnabledCounts, defaultTracingDisabledCounts},
		}

		for _, tt := range tests {
			test := propagationTest{
				forwardCount:      tt.forwardCount,
				tracingDisabled:   false,
				format:            tt.format,
				expectedBaggage:   tt.expectedBaggage,
				expectedSpanCount: tt.expectedSpanCountTracingEnabled[tracer.tracerType]}

			handler.testTracingPropagationWithEncoding(t, tracer, test)

			test.tracingDisabled = true
			test.expectedSpanCount = tt.expectedSpanCountTracingDisabled[tracer.tracerType]
			handler.testTracingPropagationWithEncoding(t, tracer, test)
		}
	})
}

func (h *traceHandler) testTracingPropagationWithEncoding(
	t *testing.T,
	tracer tracerChoice,
	test propagationTest,
) {
	descr := fmt.Sprintf("test %+v with tracer %+v", test, tracer)
	h.ch.Logger().Debug("")
	h.ch.Logger().Debugf("======> STARTING %s", descr)

	tracer.resetSpans()

	span := h.ch.Tracer().StartSpan("client")
	span.SetBaggageItem(baggageKey, baggageValue)
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	ctxBuilder := NewContextBuilder(2 * time.Second).SetParentContext(ctx)
	if test.tracingDisabled {
		ctxBuilder.DisableTracing()
	}
	ctx, cancel := ctxBuilder.Build()
	defer cancel()

	req := &TracingRequest{ForwardCount: test.forwardCount}
	var response TracingResponse
	if test.format == Raw {
		_, arg3, _, err := raw.Call(ctx, h.ch, h.ch.PeerInfo().HostPort, h.ch.PeerInfo().ServiceName,
			"rawcall", nil, req.toRaw())
		require.NoError(t, err)
		response.fromRaw(t, arg3)
	} else if test.format == JSON {
		jctx := json.Wrap(ctx)
		peer := h.ch.Peers().GetOrAdd(h.ch.PeerInfo().HostPort)
		err := json.CallPeer(jctx, peer, h.ch.PeerInfo().ServiceName, "call", req, &response)
		require.NoError(t, err)
	} else if test.format == Thrift {
		tctx := thrift.Wrap(ctx)
		res, err := h.thriftClient.Call(tctx, req.toThrift())
		require.NoError(t, err)
		response.fromThrift(t, res)
	}
	h.ch.Logger().Debugf("Top test response %+v", response)

	// Spans are finished in inbound.doneSending() or outbound.doneReading(),
	// which are called on different go-routines and may execute *after* the
	// response has been received by the client. Give them a chance to run.
	for i := 0; i < 100; i++ {
		if spanCount := tracer.spansRecorded(); spanCount == test.expectedSpanCount {
			break
		}
		time.Sleep(time.Millisecond) // max wait: 100ms
	}
	spanCount := tracer.spansRecorded()
	h.ch.Logger().Debugf("end span count: %d", spanCount)

	// finish span after taking count of recorded spans, as we're only interested
	// in the count of spans created by RPC calls.
	span.Finish()

	root := new(TracingResponse).observeSpan(ctx)

	if !tracer.isFake {
		assert.Equal(t, uint64(0), root.ParentID)
		assert.NotEqual(t, uint64(0), root.TraceID)
	}

	assert.Equal(t, test.expectedSpanCount, spanCount, "Wrong span count; %s", descr)

	for r, cnt := &response, 0; r != nil || cnt <= test.forwardCount; r, cnt = r.Child, cnt+1 {
		require.NotNil(t, r, "Expecting response for forward=%d; %s", cnt, descr)
		if !tracer.isFake {
			if tracer.zipkinCompatible || test.format != Raw {
				assert.Equal(t, root.TraceID, r.TraceID, "traceID should be the same; %s", descr)
			}
			assert.Equal(t, test.expectedBaggage, r.Luggage, "baggage should propagate; %s", descr)
		}
	}
}

func TestTracingInjectorExtractor(t *testing.T) {
	reporter := jaeger.NewInMemoryReporter()
	tracer, tCloser := jaeger.NewTracer(testutils.DefaultServerName,
		jaeger.NewConstSampler(true), reporter)
	defer tCloser.Close()

	sp := tracer.StartSpan("x")
	tsp := &Span{}
	err := tracer.Inject(sp, ZipkinSpanFormat, tsp)
	require.NoError(t, err)

	assert.NotEqual(t, uint64(0), tsp.TraceID())
	assert.NotEqual(t, uint64(0), tsp.SpanID())

	sp2, err := tracer.Join("z", ZipkinSpanFormat, tsp)
	require.NoError(t, err)
	require.NotNil(t, sp2)
}

func TestTracingSpans(t *testing.T) {
	basicRecorder := basictracer.NewInMemoryRecorder()
	basicTracer := basictracer.NewWithOptions(basictracer.Options{
		ShouldSample: func(traceID uint64) bool { return true },
		Recorder:     &basicTracerLoggingRecorder{basicRecorder},
	})

	opts := &testutils.ChannelOpts{
		ChannelOptions: ChannelOptions{Tracer: basicTracer},
		DisableRelay:   true,
	}
	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		handler := &traceHandler{t: t, ch: ch}
		// Register JSON handler
		jsonHandler := &JSONHandler{*handler}
		json.Register(ch, json.Handlers{"call": jsonHandler.callJSON}, jsonHandler.onError)

		span := ch.Tracer().StartSpan("client")
		span.SetBaggageItem(baggageKey, baggageValue)
		ctx := opentracing.ContextWithSpan(context.Background(), span)
		root := new(TracingResponse).observeSpan(ctx)

		ctx, cancel := NewContextBuilder(2 * time.Second).SetParentContext(ctx).Build()
		defer cancel()

		peer := ch.Peers().GetOrAdd(ch.PeerInfo().HostPort)
		var response TracingResponse
		require.NoError(t, json.CallPeer(json.Wrap(ctx), peer, ch.PeerInfo().ServiceName,
			"call", &TracingRequest{ForwardCount: 0}, &response))

		// Spans are finished in inbound.doneSending() or outbound.doneReading(),
		// which are called on different go-routines and may execute *after* the
		// response has been received by the client. Give them a chance to finish.
		for i := 0; i < 100; i++ {
			if spanCount := len(basicRecorder.GetSampledSpans()); spanCount == 2 {
				break
			}
			time.Sleep(time.Millisecond) // max wait: 100ms
		}
		spans := basicRecorder.GetSampledSpans()
		spanCount := len(spans)
		ch.Logger().Debugf("end span count: %d", spanCount)

		// finish span after taking count of recorded spans
		span.Finish()

		require.Equal(t, 2, spanCount, "Wrong span count")
		assert.Equal(t, root.TraceID, response.TraceID, "Trace ID must match root span")
		assert.Equal(t, baggageValue, response.Luggage, "Baggage must match")

		parent := spans[1]
		child := spans[0]

		assert.Equal(t, parent.Context.TraceID, child.Context.TraceID)
		assert.Equal(t, parent.Context.SpanID, child.Context.ParentSpanID)
		assert.True(t, parent.Context.Sampled)
		assert.True(t, child.Context.Sampled)
		assert.Equal(t, "testService::call", parent.Operation)
		assert.Equal(t, "testService::call", child.Operation)
		assert.EqualValues(t, "client", parent.Tags["span.kind"])
		assert.EqualValues(t, "server", child.Tags["span.kind"])
		assert.Equal(t, "testService", parent.Tags["peer.service"])
		assert.Equal(t, "testService", child.Tags["peer.service"])
		assert.Equal(t, "json", parent.Tags["as"])
		assert.Equal(t, "json", child.Tags["as"])
		assert.NotNil(t, parent.Tags["peer.hostname"])
		assert.NotNil(t, child.Tags["peer.hostname"])
		assert.NotNil(t, parent.Tags["peer.port"])
		assert.NotNil(t, child.Tags["peer.port"])
	})
}
