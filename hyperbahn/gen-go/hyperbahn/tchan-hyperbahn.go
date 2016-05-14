// @generated Code generated by thrift-gen. Do not modify.

// Package hyperbahn is generated code used to make or handle TChannel calls using Thrift.
package hyperbahn

import (
	"fmt"

	athrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/tchannel-go/thrift"
)

// Interfaces for the service and client for the services defined in the IDL.

// TChanHyperbahn is the interface that defines the server handler and client interface.
type TChanHyperbahn interface {
	Discover(ctx thrift.Context, query *DiscoveryQuery) (*DiscoveryResult_, error)
}

// Implementation of a client and service handler.

type tchanHyperbahnClient struct {
	thriftService string
	client        thrift.TChanClient
}

func NewTChanHyperbahnInheritedClient(thriftService string, client thrift.TChanClient) *tchanHyperbahnClient {
	return &tchanHyperbahnClient{
		thriftService,
		client,
	}
}

// NewTChanHyperbahnClient creates a client that can be used to make remote calls.
func NewTChanHyperbahnClient(client thrift.TChanClient) TChanHyperbahn {
	return NewTChanHyperbahnInheritedClient("Hyperbahn", client)
}

func (c *tchanHyperbahnClient) Discover(ctx thrift.Context, query *DiscoveryQuery) (*DiscoveryResult_, error) {
	var resp HyperbahnDiscoverResult
	args := HyperbahnDiscoverArgs{
		Query: query,
	}
	success, err := c.client.Call(ctx, c.thriftService, "discover", &args, &resp)
	if err == nil && !success {
		if e := resp.NoPeersAvailable; e != nil {
			err = e
		}
		if e := resp.InvalidServiceName; e != nil {
			err = e
		}
	}

	return resp.GetSuccess(), err
}

type tchanHyperbahnServer struct {
	handler TChanHyperbahn

	interceptorRunner thrift.InterceptorRunner
}

// NewTChanHyperbahnServer wraps a handler for TChanHyperbahn so it can be
// registered with a thrift.Server.
func NewTChanHyperbahnServer(handler TChanHyperbahn) thrift.TChanServer {
	return &tchanHyperbahnServer{
		handler: handler,
	}
}

func (s *tchanHyperbahnServer) Service() string {
	return "Hyperbahn"
}

func (s *tchanHyperbahnServer) Methods() []string {
	return []string{
		"discover",
	}
}

// RegisterInterceptors registers the provided interceptors with the server.
func (s *tchanHyperbahnServer) RegisterInterceptorRunner(runner thrift.InterceptorRunner) {
	s.interceptorRunner = runner
}

func (s *tchanHyperbahnServer) Handle(ctx thrift.Context, methodName string, protocol athrift.TProtocol) (bool, athrift.TStruct, error) {
	switch methodName {
	case "discover":
		return s.handleDiscover(ctx, protocol)

	default:
		return false, nil, fmt.Errorf("method %v not found in service %v", methodName, s.Service())
	}
}

func (s *tchanHyperbahnServer) handleDiscover(ctx thrift.Context, protocol athrift.TProtocol) (handled bool, resp athrift.TStruct, err error) {
	var req HyperbahnDiscoverArgs
	var res HyperbahnDiscoverResult
	const serviceMethod = "Hyperbahn::discover"

	if readErr := req.Read(protocol); readErr != nil {
		return false, nil, readErr
	}

	postRun, err := s.interceptorRunner.RunPre(ctx, serviceMethod, &req)

	defer func() {
		err = postRun(resp, err)
		if err != nil {
			resp = nil
			switch v := err.(type) {
			case *NoPeersAvailable:
				if v == nil {
					err = fmt.Errorf("Handler for noPeersAvailable returned non-nil error type *NoPeersAvailable but nil value")
				} else {
					res.NoPeersAvailable = v
					err = nil
					resp = &res
				}
			case *InvalidServiceName:
				if v == nil {
					err = fmt.Errorf("Handler for invalidServiceName returned non-nil error type *InvalidServiceName but nil value")
				} else {
					res.InvalidServiceName = v
					err = nil
					resp = &res
				}
			}
		}
	}()

	if err != nil {
		return false, nil, err
	}

	r, err :=
		s.handler.Discover(ctx, req.Query)

	if err == nil {
		res.Success = r
	}

	return err == nil, &res, err
}
