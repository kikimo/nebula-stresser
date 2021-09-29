package client

import (
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

type MetaOption struct {
	Timeout    time.Duration
	BufferSize int
}

func NewMetaClient(addr string, opt MetaOption) (*meta.MetaServiceClient, error) {
	timeout := thrift.SocketTimeout(opt.Timeout)
	// bufferSize := 128 << 10
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to create a net.Conn-backed Transport,: %s", err.Error())
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(opt.BufferSize)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()
	metaClient := meta.NewMetaServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := metaClient.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %s", err.Error())
	}

	if !metaClient.IsOpen() {
		return nil, fmt.Errorf("transport is off")
	}

	return metaClient, nil
}
