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
	gojson "encoding/json"
	"fmt"
	"log"
	"sync"
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
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"golang.org/x/net/context"
)

type TracingRequest struct {
	ForwardCount int
}

type TracingResponse struct {
	TraceID        uint64
	SpanID         uint64
	ParentID       uint64
	TracingEnabled bool
	Child          *TracingResponse
	Luggage        string
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
	downstream func(ctx context.Context) *TracingResponse,
) (*TracingResponse, error) {
	log.Printf("handleCall(%+v)", req)
	tchanSpan := CurrentSpan(ctx)
	if tchanSpan == nil {
		log.Printf("tracing not found")
		return nil, fmt.Errorf("tracing not found")
	}
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		log.Printf("open tracing not found")
		return nil, fmt.Errorf("tracing not found")
	}

	var childResp *TracingResponse
	if req.ForwardCount > 0 {
		childResp = downstream(ctx)
	}

	resp := &TracingResponse{
		TraceID:        tchanSpan.TraceID(),
		SpanID:         tchanSpan.SpanID(),
		ParentID:       tchanSpan.ParentID(),
		TracingEnabled: tchanSpan.Flags()&1 == 1,
		Child:          childResp,
		Luggage:        span.BaggageItem(baggageKey),
	}
	log.Printf("handleCall() -> %+v", resp)
	return resp, nil
}

// handleStringCall is a wrapper around typed handleCall which expects res to be JSON strings
func (h *traceHandler) handleStringCall(
	ctx context.Context,
	req *TracingRequest,
	downstream func(ctx context.Context) string,
) (*TracingResponse, error) {
	return h.handleCall(ctx, req, func(ctx context.Context) *TracingResponse {
		log.Printf("handleStringCall req=%+v\n", req)
		str := downstream(ctx)
		var res = &TracingResponse{}
		err := gojson.Unmarshal([]byte(str), res)
		require.NoError(h.t, err)
		return res
	})
}

// RawHandler tests tracing over Raw encoding
type RawHandler struct {
	traceHandler
}

func (h *RawHandler) Handle(ctx context.Context, args *raw.Args) (*raw.Res, error) {
	log.Printf("raw handler processing request %+v\n", args)
	req := &TracingRequest{ForwardCount: int(args.Arg3[0])}
	res, err := h.handleStringCall(ctx, req, func(ctx context.Context) string {
		_, arg3, _, err := raw.Call(ctx, h.ch, h.ch.PeerInfo().HostPort, h.ch.PeerInfo().ServiceName,
			"rawcall", nil, []byte{0})
		require.NoError(h.t, err)
		return string(arg3) // return JSON string
	})
	require.NoError(h.t, err)
	jsonBytes, err := gojson.Marshal(res)
	require.NoError(h.t, err)
	resp := &raw.Res{Arg3: jsonBytes} // return Data with JSON string
	log.Printf("raw handler returning json %s", string(jsonBytes))
	log.Printf("raw handler returning response %+v", resp)
	return resp, nil
}

func (h *RawHandler) OnError(ctx context.Context, err error) {}

// JSONHandler tests tracing over JSON encoding
type JSONHandler struct {
	traceHandler
}

func (h *JSONHandler) callJSON(ctx json.Context, req *TracingRequest) (*TracingResponse, error) {
	return h.handleCall(ctx, req, func(ctx context.Context) *TracingResponse {
		jctx := ctx.(json.Context)
		var childResp *TracingResponse
		sc := h.ch.Peers().GetOrAdd(h.ch.PeerInfo().HostPort)
		childResp = new(TracingResponse)
		require.NoError(h.t, json.CallPeer(jctx, sc, h.ch.PeerInfo().ServiceName, "call", nil, childResp))
		return childResp
	})
}

func (h *JSONHandler) onError(ctx context.Context, err error) {
	h.t.Errorf("onError %v", err)
}

// ThriftHandler tests tracing over Thrift encoding
type ThriftHandler struct {
	traceHandler
}

func (h *ThriftHandler) Call(ctx thrift.Context, arg *gen.Data) (*gen.Data, error) {
	log.Printf("tchannel handler processing request %+v\n", arg)
	req := &TracingRequest{ForwardCount: int(arg.I3)}
	res, err := h.handleStringCall(ctx, req, func(ctx context.Context) string {
		tctx := ctx.(thrift.Context)
		res, err := h.thriftClient.Call(tctx, &gen.Data{})
		require.NoError(h.t, err)
		return res.S2 // return JSON string
	})
	require.NoError(h.t, err)
	jsonBytes, err := gojson.Marshal(res)
	require.NoError(h.t, err)
	resp := &gen.Data{S2: string(jsonBytes)} // return Data with JSON string
	log.Printf("tchannel handler returning response %+v\n", resp)
	return resp, nil
}

func (h *ThriftHandler) Simple(ctx thrift.Context) error {
	return nil
}

// TODO validate tracingDisabled scenarios
type BasicTracerInMemorySpanRecorder struct {
	sync.Mutex
	spans []basictracer.RawSpan
}

func (r *BasicTracerInMemorySpanRecorder) RecordSpan(span basictracer.RawSpan) {
	r.Lock()
	defer r.Unlock()
	r.spans = append(r.spans, span)
}

type tracerChoice struct {
	tracer           opentracing.Tracer
	zipkinCompatible bool
	supportsBaggage  bool
	description      string
}

func TestTracingPropagation(t *testing.T) {
	jaeger, jCloser := jaeger.NewTracer(testutils.DefaultServerName,
		jaeger.NewConstSampler(true), jaeger.NewLoggingReporter(jaeger.StdLogger))
	defer jCloser.Close()

	basic := basictracer.NewWithOptions(basictracer.Options{
		ShouldSample:         func(traceID uint64) bool { return true },
		Recorder:             &BasicTracerInMemorySpanRecorder{},
		NewSpanEventListener: func() func(basictracer.SpanEvent) { return nil },
	})

	// TODO test does not work with Raw encoding except with Jaeger
	tracers := []tracerChoice{
		{nil, false, false, "default Noop tracer"},
		{basic, false, true, "basic tracer"},
		{jaeger, true, true, "Jaeger tracer"},
	}
	for _, tracer := range tracers {
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

			tests := []struct {
				format          Format
				forwardCount    int
				tracingEnabled  bool
				expectedBaggage string
			}{
				{Raw, 1, true, ""},  // Raw has no headers, thus no baggage
				{Raw, 1, false, ""}, // Raw has no headers, thus no baggage
				{JSON, 1, true, baggageValue},
				{JSON, 1, false, baggageValue},
				{Thrift, 1, true, baggageValue},
				{Thrift, 1, false, baggageValue},
			}

			for _, test := range tests {
				handler.testTracingPropagation(t, tracer, test)
			}
		})
	}
}

func (h *traceHandler) testTracingPropagation(
	t *testing.T,
	tracer tracerChoice,
	test struct {
		format          Format
		forwardCount    int
		tracingEnabled  bool
		expectedBaggage string
	},
) {
	descr := fmt.Sprintf("test %+v with tracer %+v", test, tracer)
	if !tracer.zipkinCompatible && test.format == Raw {
		log.Printf("Raw encoding not supported; %s", descr)
		return
	}
	log.Printf("======> Starting %s", descr)

	ctx, cancel := json.NewContext(2 * time.Second)
	defer cancel()

	span := h.ch.Tracer().StartSpan("client")
	span.SetBaggageItem(baggageKey, baggageValue)
	if !test.tracingEnabled {
		ext.SamplingPriority.Set(span, 0)
	}
	ctx2 := opentracing.ContextWithSpan(ctx, span)

	var response TracingResponse
	if test.format == JSON {
		jctx := json.Wrap(ctx2)
		peer := h.ch.Peers().GetOrAdd(h.ch.PeerInfo().HostPort)
		req := &TracingRequest{ForwardCount: test.forwardCount}
		err := json.CallPeer(jctx, peer, h.ch.PeerInfo().ServiceName, "call", req, &response)
		require.NoError(t, err)
	} else if test.format == Thrift {
		tctx := thrift.Wrap(ctx2)
		req := &gen.Data{I3: int32(test.forwardCount)}
		res, err := h.thriftClient.Call(tctx, req)
		require.NoError(t, err)
		err = gojson.Unmarshal([]byte(res.S2), &response)
		require.NoError(t, err)
	} else if test.format == Raw {
		_, arg3, _, err := raw.Call(ctx2, h.ch, h.ch.PeerInfo().HostPort, h.ch.PeerInfo().ServiceName,
			"rawcall", nil, []byte{byte(test.forwardCount)})
		require.NoError(t, err)
		err = gojson.Unmarshal(arg3, &response)
		require.NoError(t, err)
	}
	log.Printf("Top test response %+v", response)

	span.Finish()

	rootSpan := CurrentSpan(ctx2)
	log.Printf("Current span after test: %+v\n", rootSpan)
	require.NotNil(t, rootSpan)

	// TODO instead of Zipkin-style, check tracingEnabled via reporter
	if tracer.zipkinCompatible {
		assert.Equal(t, uint64(0), rootSpan.ParentID())
		assert.NotEqual(t, uint64(0), rootSpan.TraceID())
		assert.Equal(t, test.tracingEnabled, rootSpan.Flags()&1 == 1, "Tracing should be enabled; %s", descr)
	}

	for r, cnt := &response, 0; r != nil || cnt <= test.forwardCount; r, cnt = r.Child, cnt+1 {
		require.NotNil(t, r, "No response for forward=%d; %s", cnt, descr)
		if tracer.zipkinCompatible {
			assert.Equal(t, test.tracingEnabled, r.TracingEnabled, "Tracing should be enabled; %s", descr)
			assert.Equal(t, rootSpan.TraceID(), r.TraceID, "traceID sjould be the same; %s", descr)
		}
		if tracer.supportsBaggage {
			assert.Equal(t, test.expectedBaggage, r.Luggage, "baggage propagation check; %s", descr)
		}
	}

	//nestedResponse := response.Child
	//require.NotNil(t, nestedResponse)
	//assert.Equal(t, rootSpan.TraceID(), nestedResponse.TraceID)
	//assert.Equal(t, response.SpanID, nestedResponse.ParentID)
	//// assert.True(t, response.TracingEnabled, "Tracing should be enabled")
	//assert.NotEqual(t, response.SpanID, nestedResponse.SpanID)
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

//func TestTraceReportingEnabled(t *testing.T) {
//	initialTime := time.Date(2015, 2, 1, 10, 10, 0, 0, time.UTC)
//
//	var state struct {
//		signal chan struct{}
//
//		call TraceData
//		span Span
//	}
//	testTraceReporter := TraceReporterFunc(func(data TraceData) {
//		defer close(state.signal)
//
//		span := data.Span
//		data.Span = Span{}
//		state.call = data
//		state.span = span
//	})
//
//	traceReporterOpts := testutils.NewOpts().SetTraceReporter(testTraceReporter)
//	tests := []struct {
//		name       string
//		serverOpts *testutils.ChannelOpts
//		clientOpts *testutils.ChannelOpts
//		expected   []Annotation
//		fromServer bool
//	}{
//		{
//			name:       "inbound",
//			serverOpts: traceReporterOpts,
//			expected: []Annotation{
//				{Key: "sr", Timestamp: initialTime.Add(2 * time.Second)},
//				{Key: "ss", Timestamp: initialTime.Add(3 * time.Second)},
//			},
//			fromServer: true,
//		},
//		{
//			name:       "outbound",
//			clientOpts: traceReporterOpts,
//			expected: []Annotation{
//				{Key: "cs", Timestamp: initialTime.Add(time.Second)},
//				{Key: "cr", Timestamp: initialTime.Add(6 * time.Second)},
//			},
//		},
//	}
//
//	for _, tt := range tests {
//		serverNow, serverNowFn := testutils.NowStub(initialTime.Add(time.Second))
//		clientNow, clientNowFn := testutils.NowStub(initialTime)
//		serverNowFn(time.Second)
//		clientNowFn(time.Second)
//
//		// Note: we disable the relay as the relay shares the same options
//		// and since the relay would call timeNow, it causes a mismatch in
//		// the expected timestamps.
//		tt.serverOpts = testutils.DefaultOpts(tt.serverOpts).SetTimeNow(serverNow).NoRelay()
//		tt.clientOpts = testutils.DefaultOpts(tt.clientOpts).SetTimeNow(clientNow)
//
//		WithVerifiedServer(t, tt.serverOpts, func(ch *Channel, hostPort string) {
//			state.signal = make(chan struct{})
//
//			testutils.RegisterEcho(ch, func() {
//				clientNowFn(5 * time.Second)
//			})
//
//			clientCh := testutils.NewClient(t, tt.clientOpts)
//			defer clientCh.Close()
//			ctx, cancel := NewContext(time.Second)
//			defer cancel()
//
//			_, _, _, err := raw.Call(ctx, clientCh, hostPort, ch.PeerInfo().ServiceName, "echo", nil, []byte("arg3"))
//			require.NoError(t, err, "raw.Call failed")
//
//			binaryAnnotations := []BinaryAnnotation{
//				{"cn", clientCh.PeerInfo().ServiceName},
//				{"as", Raw.String()},
//			}
//			target := TraceEndpoint{
//				HostPort:    hostPort,
//				ServiceName: ch.ServiceName(),
//			}
//			source := target
//			if !tt.fromServer {
//				source = TraceEndpoint{
//					HostPort:    "0.0.0.0:0",
//					ServiceName: clientCh.ServiceName(),
//				}
//			}
//
//			select {
//			case <-state.signal:
//			case <-time.After(time.Second):
//				t.Fatalf("Did not receive trace report within timeout")
//			}
//
//			expected := TraceData{Annotations: tt.expected, BinaryAnnotations: binaryAnnotations, Source: source, Target: target, Method: "echo"}
//			assert.Equal(t, expected, state.call, "%v: Report args mismatch", tt.name)
//			curSpan := CurrentSpan(ctx)
//			assert.Equal(t, NewSpan(curSpan.TraceID(), curSpan.TraceID(), 0), state.span, "Span mismatch")
//		})
//	}
//}

//func TestTraceReportingDisabled(t *testing.T) {
//	var gotCalls int
//	testTraceReporter := TraceReporterFunc(func(_ TraceData) {
//		gotCalls++
//	})
//
//	traceReporterOpts := testutils.NewOpts().SetTraceReporter(testTraceReporter)
//	WithVerifiedServer(t, traceReporterOpts, func(ch *Channel, hostPort string) {
//		ch.Register(raw.Wrap(newTestHandler(t)), "echo")
//
//		ctx, cancel := NewContext(time.Second)
//		defer cancel()
//
//		CurrentSpan(ctx).EnableTracing(false)
//		_, _, _, err := raw.Call(ctx, ch, hostPort, ch.PeerInfo().ServiceName, "echo", nil, []byte("arg3"))
//		require.NoError(t, err, "raw.Call failed")
//
//		assert.Equal(t, 0, gotCalls, "TraceReporter should not report if disabled")
//	})
//}

//func TestTraceSamplingRate(t *testing.T) {
//	rand.Seed(10)
//
//	tests := []struct {
//		sampleRate  float64 // if this is < 0, then the value is not set.
//		count       int
//		expectedMin int
//		expectedMax int
//	}{
//		{1.0, 100, 100, 100},
//		{0.5, 100, 40, 60},
//		{0.1, 100, 5, 15},
//		{0, 100, 0, 0},
//		{-1, 100, 100, 100}, // default of 1.0 should be used.
//	}
//
//	for _, tt := range tests {
//		WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
//			var reportedTraces int
//			testTraceReporter := TraceReporterFunc(func(_ TraceData) {
//				reportedTraces++
//			})
//
//			var tracedCalls int
//			testutils.RegisterFunc(ch, "t", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
//				if CurrentSpan(ctx).TracingEnabled() {
//					tracedCalls++
//				}
//
//				return &raw.Res{}, nil
//			})
//
//			opts := testutils.NewOpts().SetTraceReporter(testTraceReporter)
//			if tt.sampleRate >= 0 {
//				opts.SetTraceSampleRate(tt.sampleRate)
//			}
//
//			client := testutils.NewClient(t, opts)
//			defer client.Close()
//
//			for i := 0; i < tt.count; i++ {
//				ctx, cancel := NewContext(time.Second)
//				defer cancel()
//
//				_, _, _, err := raw.Call(ctx, client, hostPort, ch.PeerInfo().ServiceName, "t", nil, nil)
//				require.NoError(t, err, "raw.Call failed")
//			}
//
//			assert.Equal(t, reportedTraces, tracedCalls,
//				"Number of traces report doesn't match calls with tracing enabled")
//			assert.True(t, tracedCalls >= tt.expectedMin,
//				"Number of trace enabled calls (%v) expected to be greater than %v", tracedCalls, tt.expectedMin)
//			assert.True(t, tracedCalls <= tt.expectedMax,
//				"Number of trace enabled calls (%v) expected to be less than %v", tracedCalls, tt.expectedMax)
//		})
//	}
//}
//
//func TestChildCallsNotSampled(t *testing.T) {
//	var traceEnabledCalls int
//
//	s1 := testutils.NewServer(t, testutils.NewOpts().SetTraceSampleRate(0.0001))
//	defer s1.Close()
//	s2 := testutils.NewServer(t, nil)
//	defer s2.Close()
//
//	testutils.RegisterFunc(s1, "s1", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
//		_, _, _, err := raw.Call(ctx, s1, s2.PeerInfo().HostPort, s2.ServiceName(), "s2", nil, nil)
//		require.NoError(t, err, "raw.Call from s1 to s2 failed")
//		return &raw.Res{}, nil
//	})
//
//	testutils.RegisterFunc(s2, "s2", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
//		if CurrentSpan(ctx).TracingEnabled() {
//			traceEnabledCalls++
//		}
//		return &raw.Res{}, nil
//	})
//
//	client := testutils.NewClient(t, nil)
//	defer client.Close()
//
//	const numCalls = 100
//	for i := 0; i < numCalls; i++ {
//		ctx, cancel := NewContext(time.Second)
//		defer cancel()
//
//		_, _, _, err := raw.Call(ctx, client, s1.PeerInfo().HostPort, s1.ServiceName(), "s1", nil, nil)
//		require.NoError(t, err, "raw.Call to s1 failed")
//	}
//
//	// Even though s1 has sampling enabled, it should not affect incoming calls.
//	assert.Equal(t, numCalls, traceEnabledCalls, "Trace sampling should not inbound calls")
//}
