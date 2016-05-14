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

package thrift_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	// Test is in a separate package to avoid circular dependencies.
	. "github.com/uber/tchannel-go/thrift"

	"github.com/apache/thrift/lib/go/thrift"
	athrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/testutils"
	gen "github.com/uber/tchannel-go/thrift/gen-go/test"
	"github.com/uber/tchannel-go/thrift/mocks"
)

// Generate the service mocks using go generate.
//go:generate mockery -name TChanSimpleService
//go:generate mockery -name TChanSecondService

type testArgs struct {
	server *Server
	s1     *mocks.TChanSimpleService
	s2     *mocks.TChanSecondService
	c1     gen.TChanSimpleService
	c2     gen.TChanSecondService
}

func ctxArg() mock.AnythingOfTypeArgument {
	return mock.AnythingOfType("tchannel.headerCtx")
}

func TestThriftArgs(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		arg := &gen.Data{
			B1: true,
			S2: "str",
			I3: 102,
		}
		ret := &gen.Data{
			B1: false,
			S2: "return-str",
			I3: 105,
		}

		args.s1.On("Call", ctxArg(), arg).Return(ret, nil)
		got, err := args.c1.Call(ctx, arg)
		require.NoError(t, err)
		assert.Equal(t, ret, got)
	})
}

func TestRequest(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		args.s1.On("Simple", ctxArg()).Return(nil)
		require.NoError(t, args.c1.Simple(ctx))
	})
}

func TestRetryRequest(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		count := 0
		args.s1.On("Simple", ctxArg()).Return(tchannel.ErrServerBusy).
			Run(func(args mock.Arguments) {
				count++
			})
		require.Error(t, args.c1.Simple(ctx), "Simple expected to fail")
		assert.Equal(t, 5, count, "Expected Simple to be retried 5 times")
	})
}

func TestRequestSubChannel(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	tchan := testutils.NewServer(t, testutils.NewOpts().SetServiceName("svc1"))
	defer tchan.Close()

	clientCh := testutils.NewClient(t, nil)
	defer clientCh.Close()
	clientCh.Peers().Add(tchan.PeerInfo().HostPort)

	tests := []tchannel.Registrar{tchan, tchan.GetSubChannel("svc2"), tchan.GetSubChannel("svc3")}
	for _, ch := range tests {
		mockHandler := new(mocks.TChanSecondService)
		server := NewServer(ch)
		server.Register(gen.NewTChanSecondServiceServer(mockHandler))

		client := NewClient(clientCh, ch.ServiceName(), nil)
		secondClient := gen.NewTChanSecondServiceClient(client)

		echoArg := ch.ServiceName()
		echoRes := echoArg + "-echo"
		mockHandler.On("Echo", ctxArg(), echoArg).Return(echoRes, nil)
		res, err := secondClient.Echo(ctx, echoArg)
		assert.NoError(t, err, "Echo failed")
		assert.Equal(t, echoRes, res)
	}
}

func TestLargeRequest(t *testing.T) {
	arg := testutils.RandString(100000)
	res := strings.ToLower(arg)

	fmt.Println(len(arg))
	withSetup(t, func(ctx Context, args testArgs) {
		args.s2.On("Echo", ctxArg(), arg).Return(res, nil)

		got, err := args.c2.Echo(ctx, arg)
		if assert.NoError(t, err, "Echo got error") {
			assert.Equal(t, res, got, "Echo got unexpected response")
		}
	})
}

func TestThriftError(t *testing.T) {
	thriftErr := &gen.SimpleErr{
		Message: "this is the error",
	}
	withSetup(t, func(ctx Context, args testArgs) {
		args.s1.On("Simple", ctxArg()).Return(thriftErr)
		got := args.c1.Simple(ctx)
		require.Error(t, got)
		require.Equal(t, thriftErr, got)
	})
}

func TestThriftNilErr(t *testing.T) {
	var thriftErr *gen.SimpleErr
	withSetup(t, func(ctx Context, args testArgs) {
		args.s1.On("Simple", ctxArg()).Return(thriftErr)
		got := args.c1.Simple(ctx)
		require.Error(t, got)
		require.Contains(t, got.Error(), "non-nil error type")
		require.Contains(t, got.Error(), "nil value")
	})
}

func TestUnknownError(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		args.s1.On("Simple", ctxArg()).Return(errors.New("unexpected err"))
		got := args.c1.Simple(ctx)
		require.Error(t, got)
		require.Equal(t, tchannel.NewSystemError(tchannel.ErrCodeUnexpected, "unexpected err"), got)
	})
}

func TestMultiple(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		args.s1.On("Simple", ctxArg()).Return(nil)
		args.s2.On("Echo", ctxArg(), "test1").Return("test2", nil)

		require.NoError(t, args.c1.Simple(ctx))
		res, err := args.c2.Echo(ctx, "test1")
		require.NoError(t, err)
		require.Equal(t, "test2", res)
	})
}

func TestHeaders(t *testing.T) {
	reqHeaders := map[string]string{"header1": "value1", "header2": "value2"}
	respHeaders := map[string]string{"resp1": "value1-resp", "resp2": "value2-resp"}

	withSetup(t, func(ctx Context, args testArgs) {
		args.s1.On("Simple", ctxArg()).Return(nil).Run(func(args mock.Arguments) {
			ctx := args.Get(0).(Context)
			assert.Equal(t, reqHeaders, ctx.Headers(), "request headers mismatch")
			ctx.SetResponseHeaders(respHeaders)
		})

		ctx = WithHeaders(ctx, reqHeaders)
		require.NoError(t, args.c1.Simple(ctx))
		assert.Equal(t, respHeaders, ctx.ResponseHeaders(), "response headers mismatch")
	})
}

func TestClientHostPort(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	s1ch := testutils.NewServer(t, nil)
	s2ch := testutils.NewServer(t, nil)
	defer s1ch.Close()
	defer s2ch.Close()

	s1ch.Peers().Add(s2ch.PeerInfo().HostPort)
	s2ch.Peers().Add(s1ch.PeerInfo().HostPort)

	mock1, mock2 := new(mocks.TChanSecondService), new(mocks.TChanSecondService)
	NewServer(s1ch).Register(gen.NewTChanSecondServiceServer(mock1))
	NewServer(s2ch).Register(gen.NewTChanSecondServiceServer(mock2))

	// When we call using a normal client, it can only call the other server (only peer).
	c1 := gen.NewTChanSecondServiceClient(NewClient(s1ch, s2ch.PeerInfo().ServiceName, nil))
	mock2.On("Echo", ctxArg(), "call1").Return("call1", nil)
	res, err := c1.Echo(ctx, "call1")
	assert.NoError(t, err, "call1 failed")
	assert.Equal(t, "call1", res)

	// When we call using a client that specifies host:port, it should call that server.
	c2 := gen.NewTChanSecondServiceClient(
		NewClient(s1ch, s1ch.PeerInfo().ServiceName, &ClientOptions{
			HostPort: s1ch.PeerInfo().HostPort,
		}),
	)
	mock1.On("Echo", ctxArg(), "call2").Return("call2", nil)
	res, err = c2.Echo(ctx, "call2")
	assert.NoError(t, err, "call2 failed")
	assert.Equal(t, "call2", res)
}

func TestRegisterPostResponseCB(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		arg := &gen.Data{
			B1: true,
			S2: "str",
			I3: 102,
		}
		ret := &gen.Data{
			B1: false,
			S2: "return-str",
			I3: 105,
		}

		called := make(chan struct{})
		cb := func(method string, response thrift.TStruct) {
			assert.Equal(t, "Call", method)
			res, ok := response.(*gen.SimpleServiceCallResult)
			if assert.True(t, ok, "response type should be Result struct") {
				assert.Equal(t, ret, res.GetSuccess(), "result should be returned value")
			}
			close(called)
		}
		args.server.Register(gen.NewTChanSimpleServiceServer(args.s1), OptPostResponse(cb))

		args.s1.On("Call", ctxArg(), arg).Return(ret, nil)
		res, err := args.c1.Call(ctx, arg)
		require.NoError(t, err, "Call failed")
		assert.Equal(t, res, ret, "Call return value wrong")
		select {
		case <-time.After(time.Second):
			t.Errorf("post-response callback not called")
		case <-called:
		}
	})
}

func TestThriftTimeout(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		handler := make(chan struct{})

		args.s2.On("Echo", ctxArg(), "asd").Return("asd", nil).Run(func(args mock.Arguments) {
			time.Sleep(testutils.Timeout(15 * time.Millisecond))
			close(handler)
		})

		ctx, cancel := NewContext(testutils.Timeout(10 * time.Millisecond))
		defer cancel()

		_, err := args.c2.Echo(ctx, "asd")
		assert.Equal(t, err, tchannel.ErrTimeout, "Expect call to time out")

		// Wait for the handler to return, otherwise the test ends before the Server gets an error.
		select {
		case <-handler:
		case <-time.After(time.Second):
			t.Errorf("Echo handler did not run")
		}
	})
}

func TestThriftContextFn(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		args.server.SetContextFn(func(
			ctx context.Context, method string, headers map[string]string,
		) Context {
			return WithHeaders(ctx, map[string]string{"custom": "headers"})
		})

		args.s2.On("Echo", ctxArg(), "test").Return("test", nil).Run(func(args mock.Arguments) {
			ctx := args.Get(0).(Context)
			assert.Equal(t, "headers", ctx.Headers()["custom"], "Custom header is missing")
		})
		_, err := args.c2.Echo(ctx, "test")
		assert.NoError(t, err, "Echo failed")
	})
}

type interceptor struct {
	post func(
		ctx Context, method string, args, response athrift.TStruct, err error,
	) error
	pre func(ctx Context, method string, args athrift.TStruct) error
}

func (i interceptor) Pre(ctx Context, method string, args athrift.TStruct) error {
	if i.pre != nil {
		return i.pre(ctx, method, args)
	}
	return nil
}

func (i interceptor) Post(
	ctx Context, method string, args, response athrift.TStruct, err error,
) error {
	if i.post != nil {
		return i.post(ctx, method, args, response, err)
	}
	return err
}

func TestPreInterceptorCallOrderAndArguments(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		serviceMethod := "SimpleService::Throws"
		ctx, cancel := NewContext(time.Second)
		defer cancel()
		arg := "test"
		ret := "return"

		firstPreCalled := false
		secondPreCalled := false

		firstPostCalled := false
		secondPostCalled := false

		firstInterceptor := &mocks.Interceptor{}
		secondInterceptor := &mocks.Interceptor{}

		args.server.RegisterInterceptor(firstInterceptor)
		args.server.RegisterInterceptor(secondInterceptor)

		firstInterceptor.On(
			"Pre", mock.Anything, serviceMethod, mock.Anything,
		).Run(func(preArgs mock.Arguments) {
			throwsArgs := preArgs.Get(2).(*gen.SimpleServiceThrowsArgs)
			assert.Equal(t, arg, throwsArgs.Arg)
			assert.False(t, secondPreCalled)
			firstPreCalled = true
		}).Return(nil)

		secondInterceptor.On(
			"Pre", mock.Anything, serviceMethod, mock.Anything,
		).Run(func(preArgs mock.Arguments) {
			assert.True(t, firstPreCalled)
		}).Return(nil)

		args.s1.On("Throws", mock.Anything, arg).Return(ret, nil)

		firstInterceptor.On(
			"Post", mock.Anything, serviceMethod, mock.Anything, mock.Anything, nil,
		).Run(func(preArgs mock.Arguments) {
			assert.True(t, firstPreCalled)
			assert.True(t, secondPostCalled, "Second post was called first")
			firstPostCalled = true
			throwsResult := preArgs.Get(3).(*gen.SimpleServiceThrowsResult)
			assert.Equal(t, ret, throwsResult.GetSuccess())
		}).Return(nil)

		secondInterceptor.On(
			"Post", mock.Anything, serviceMethod, mock.Anything, mock.Anything, nil,
		).Run(func(preArgs mock.Arguments) {
			assert.False(t, firstPostCalled, "first post has not yet been called")
			secondPostCalled = true
		}).Return(nil)

		_, err := args.c1.Throws(ctx, arg)

		assert.NoError(t, err)
		firstInterceptor.AssertExpectations(t)
		secondInterceptor.AssertExpectations(t)
	})
}

func TestPreInterceptorShortCircuiting(t *testing.T) {
	withSetup(t, func(ctx Context, args testArgs) {
		preError := fmt.Errorf("some error")

		firstInterceptor := &mocks.Interceptor{}
		errorInterceptor := &mocks.Interceptor{}
		// This inteceptor should not get called so we call no .Ons on this mock
		uncalledInterceptor := &mocks.Interceptor{}

		firstInterceptor.On(
			"Pre", mock.Anything, mock.Anything, mock.Anything,
		).Return(nil)
		errorInterceptor.On(
			"Pre", mock.Anything, mock.Anything, mock.Anything,
		).Return(preError)

		firstTransformedError := fmt.Errorf("Tranformed")
		secondTransformedError := &gen.SimpleErr{}

		errorInterceptor.On(
			"Post", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			preError,
		).Return(firstTransformedError)
		firstInterceptor.On(
			"Post", mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			firstTransformedError,
		).Return(secondTransformedError)

		args.server.RegisterInterceptor(firstInterceptor)
		args.server.RegisterInterceptor(errorInterceptor)
		args.server.RegisterInterceptor(uncalledInterceptor)

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		result, err := args.c1.Throws(ctx, "test")

		assert.Equal(t, result, "")
		assert.Equal(t, secondTransformedError, err)

		firstInterceptor.AssertExpectations(t)
		errorInterceptor.AssertExpectations(t)
		uncalledInterceptor.AssertExpectations(t)
	})
}

func withSetup(t *testing.T, f func(ctx Context, args testArgs)) {
	args := testArgs{
		s1: new(mocks.TChanSimpleService),
		s2: new(mocks.TChanSecondService),
	}

	ctx, cancel := NewContext(time.Second)
	defer cancel()

	// Start server
	ch, server := setupServer(t, args.s1, args.s2)
	defer ch.Close()
	args.server = server

	// Get client1
	args.c1, args.c2 = getClients(t, ch)

	f(ctx, args)

	args.s1.AssertExpectations(t)
	args.s2.AssertExpectations(t)
}

func setupServer(t *testing.T, h *mocks.TChanSimpleService, sh *mocks.TChanSecondService) (*tchannel.Channel, *Server) {
	ch := testutils.NewServer(t, nil)
	server := NewServer(ch)
	server.Register(gen.NewTChanSimpleServiceServer(h))
	server.Register(gen.NewTChanSecondServiceServer(sh))
	return ch, server
}

func getClients(t *testing.T, serverCh *tchannel.Channel) (gen.TChanSimpleService, gen.TChanSecondService) {
	serverInfo := serverCh.PeerInfo()
	ch := testutils.NewClient(t, nil)

	ch.Peers().Add(serverInfo.HostPort)
	client := NewClient(ch, serverInfo.ServiceName, nil)

	simpleClient := gen.NewTChanSimpleServiceClient(client)
	secondClient := gen.NewTChanSecondServiceClient(client)
	return simpleClient, secondClient
}
