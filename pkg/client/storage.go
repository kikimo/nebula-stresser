package client

import (
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

type StorageOption struct {
	Timeout    time.Duration
	BufferSize int // in bytes
}

func NewGraphStorageServiceClient(addr string, opt StorageOption) (*storage.GraphStorageServiceClient, error) {
	return newStorageClient(addr, opt)
}

func newStorageClient(addr string, opt StorageOption) (*storage.GraphStorageServiceClient, error) {
	timeout := thrift.SocketTimeout(opt.Timeout)
	// TODO what for?
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

	client := storage.NewGraphStorageServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %s", err.Error())
	}

	if !client.IsOpen() {
		return nil, fmt.Errorf("transport is off")
	}

	return client, nil
}

type StorageClient struct {
	// *storage.GraphStorageServiceClient
	storage.GraphStorageServiceClient
	clients    []*storage.GraphStorageServiceClient
	currLeader int
}

func (c *StorageClient) Leader() int {
	return c.currLeader
}

func (c *StorageClient) updateLeader() {
	// oldLeader := c.currLeader
	c.currLeader = (c.currLeader + 1) % len(c.clients)
	// fmt.Printf("updating leader from %d to %d\n", oldLeader, c.currLeader)
}

func (c *StorageClient) leader() *storage.GraphStorageServiceClient {
	return c.clients[c.currLeader]
}

func (c *StorageClient) GetNeighbors(req *storage.GetNeighborsRequest) (_r *storage.GetNeighborsResponse, err error) {
	resp, err := c.leader().GetNeighbors(req)
	if err != nil {
		return resp, err
	}

	failedParts := resp.GetResult_().GetFailedParts()
	if len(failedParts) == 0 {
		return resp, nil
	}

	// len(failedParts) > 0 {
	fPart := failedParts[0]
	if !fPart.IsSetLeader() {
		c.updateLeader()
	}

	return resp, err
}

/*

func (c *StorageClient) GetProps(ctx context.Context, req *GetPropRequest) (_r *GetPropResponse, err error) {

}
func (c *StorageClient) AddVertices(ctx context.Context, req *AddVerticesRequest) (_r *ExecResponse, err error) {

}
func (c *StorageClient) AddEdges(ctx context.Context, req *AddEdgesRequest) (_r *ExecResponse, err error) {

}
func (c *StorageClient) DeleteEdges(ctx context.Context, req *DeleteEdgesRequest) (_r *ExecResponse, err error) {

}
func (c *StorageClient) DeleteVertices(ctx context.Context, req *DeleteVerticesRequest) (_r *ExecResponse, err error) {

}
func (c *StorageClient) DeleteTags(ctx context.Context, req *DeleteTagsRequest) (_r *ExecResponse, err error) {

}
func (c *StorageClient) UpdateVertex(ctx context.Context, req *UpdateVertexRequest) (_r *UpdateResponse, err error) {

}
func (c *StorageClient) UpdateEdge(ctx context.Context, req *UpdateEdgeRequest) (_r *UpdateResponse, err error) {

}
func (c *StorageClient) ScanVertex(ctx context.Context, req *ScanVertexRequest) (_r *ScanVertexResponse, err error) {

}
func (c *StorageClient) ScanEdge(ctx context.Context, req *ScanEdgeRequest) (_r *ScanEdgeResponse, err error) {

}
func (c *StorageClient) GetUUID(ctx context.Context, req *GetUUIDReq) (_r *GetUUIDResp, err error) {

}
func (c *StorageClient) LookupIndex(ctx context.Context, req *LookupIndexRequest) (_r *LookupIndexResp, err error) {

}
func (c *StorageClient) LookupAndTraverse(ctx context.Context, req *LookupAndTraverseRequest) (_r *GetNeighborsResponse, err error) {

}
func (c *StorageClient) ChainUpdateEdge(ctx context.Context, req *UpdateEdgeRequest) (_r *UpdateResponse, err error) {

}

*/
func (c *StorageClient) ChainAddEdges(req *storage.AddEdgesRequest) (_r *storage.ExecResponse, err error) {
	// fmt.Printf("curr leader: %d\n", c.currLeader)
	resp, err := c.leader().ChainAddEdges(req)
	if err != nil {
		return resp, err
	}

	failedParts := resp.GetResult_().GetFailedParts()
	if len(failedParts) == 0 {
		return resp, nil
	}
	// fmt.Printf("insert resp %+v, err: %+v\n", resp, err)

	// len(failedParts) > 0 {
	fPart := failedParts[0]
	if fPart.GetCode() == nebula.ErrorCode_E_LEADER_CHANGED {
		c.updateLeader()
		return resp, fmt.Errorf("wrong leader: %+v", fPart)
	}

	return resp, err
}

func NewStorageClient(metaClient *meta.MetaServiceClient, storageOpt StorageOption) (*StorageClient, error) {
	listClusterReq := &meta.ListClusterInfoReq{}
	listClusterResp, err := metaClient.ListCluster(listClusterReq)
	if err != nil {
		return nil, fmt.Errorf("failed list nebula cluster, nested error: %+v", err)
	}

	clients := []*storage.GraphStorageServiceClient{}
	storageServers := listClusterResp.GetStorageServers()
	for _, ss := range storageServers {
		host := ss.Host
		addr := fmt.Sprintf("%s:%d", host.GetHost(), host.GetPort())
		storageClient, err := newStorageClient(addr, storageOpt)
		if err != nil {
			return nil, fmt.Errorf("error init storage clients, nested error: %+v", err)
		}

		clients = append(clients, storageClient)
	}

	storageClient := &StorageClient{
		clients:    clients,
		currLeader: 2,
	}

	return storageClient, nil
}

/*
func NewStorageClientsFromMeta(metaClient *meta.MetaServiceClient, storageOpt StorageOption) ([]*storage.GraphStorageServiceClient, error) {
	listClusterReq := &meta.ListClusterInfoReq{}
	listClusterResp, err := metaClient.ListCluster(listClusterReq)
	if err != nil {
		return nil, fmt.Errorf("failed list nebula cluster, nested error: %+v", err)
	}

	clients := []*storage.GraphStorageServiceClient{}
	storageServers := listClusterResp.GetStorageServers()
	for _, ss := range storageServers {
		host := ss.Host
		addr := fmt.Sprintf("%s:%d", host.GetHost(), host.GetPort())
		storageClient, err := newStorageClient(addr, storageOpt)
		if err != nil {
			return nil, fmt.Errorf("error init storage clients, nested error: %+v", err)
		}

		clients = append(clients, storageClient)
	}

	return clients, nil
}
*/
