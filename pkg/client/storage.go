package client

import (
	"fmt"
	"math"
	"strings"
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

func newDefaultStorageClient(addr string, opt StorageOption) (*defaultStorageClient, error) {
	client := defaultStorageClient{
		addr: addr,
		opt:  opt,
	}

	if err := client.rest(); err != nil {
		return nil, fmt.Errorf("error creating storage client: %+v", err)
	}

	return &client, nil
}

func NewGraphStorageServiceClient(addr string, opt StorageOption) (*storage.GraphStorageServiceClient, error) {
	timeout := thrift.SocketTimeout(opt.Timeout)
	// TODO what for?
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		return nil, fmt.Errorf("failed creating a net.Conn-backed Transport,: %+v", err)
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(opt.BufferSize)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	client := storage.NewGraphStorageServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %+v", err)
	}

	if !client.IsOpen() {
		return nil, fmt.Errorf("transport is off")
	}

	return client, nil
}

func (c *defaultStorageClient) rest() error {
	// try close client first
	if c.GraphStorageServiceClient != nil && c.GraphStorageServiceClient.IsOpen() {
		if err := c.GraphStorageServiceClient.Close(); err != nil {
			fmt.Printf("error close storage client: %+v", err)
		}
	}

	client, err := NewGraphStorageServiceClient(c.addr, c.opt)
	if err != nil {
		return fmt.Errorf("failed resting storage client: %+v", err)
	}
	c.GraphStorageServiceClient = client

	return nil
}

type StorageClient interface {
	AddEdges(req *storage.AddEdgesRequest) (*storage.ExecResponse, error)
	ChainAddEdges(req *storage.AddEdgesRequest) (*storage.ExecResponse, error)
}

type defaultStorageClient struct {
	// metaClient *MetaClient
	addr string
	opt  StorageOption
	*storage.GraphStorageServiceClient
}

type storageClientProxy struct {
	peers      []*defaultStorageClient
	connDown   map[int]struct{}
	metaClient *MetaClient
	leader     int
}

func (s *storageClientProxy) currLeader() *defaultStorageClient {
	return s.peers[s.leader]
}

func (s *storageClientProxy) AddEdges(req *storage.AddEdgesRequest) (*storage.ExecResponse, error) {
	resp, err := s.currLeader().AddEdges(req)

	return s.checkResponse(resp, err)
}

func (s *storageClientProxy) checkResponse(resp *storage.ExecResponse, err error) (*storage.ExecResponse, error) {
	if err != nil {
		if strings.Index(err.Error(), "write tcp ") == 0 || strings.Index(err.Error(), "read tcp ") == 0 {
			fmt.Printf("error inserting edge: %+v\n", err)
			s.updateLeader()

			return resp, err
		} else if strings.Contains(err.Error(), "failed: out of sequence response") {
			fmt.Printf("error inserting edge: %+v\n", err)
			if err := s.currLeader().rest(); err != nil {
				fmt.Printf("error resting leader conn: %+v", err)
			}

			return resp, err
		} else {
			// clients[cid].UpdateLeader()
			// time.Sleep(1 * time.Second)
			// fmt.Printf("fuck error inserting edge: %+v\n", err)
			panic(fmt.Sprintf("fuck error inserting edge: %+v\n", err))
			// metaLock.Lock()
			// clients[cid], err = client.NewStorageClient(metaClient, storageOpt)
			// metaLock.Unlock()
			// if err != nil {
			// 	fmt.Printf("failed creating new client: %+v", err)
			// }
		}
	}

	failedParts := resp.GetResult_().GetFailedParts()
	if len(failedParts) == 0 {
		return resp, nil
	}
	// fmt.Printf("insert resp %+v, err: %+v\n", resp, err)

	// len(failedParts) > 0 {
	fPart := failedParts[0]
	switch fPart.GetCode() {
	case nebula.ErrorCode_E_LEADER_CHANGED:
		s.updateLeader()
	case nebula.ErrorCode_E_WRITE_WRITE_CONFLICT:
	case nebula.ErrorCode_E_CONSENSUS_ERROR:
	case nebula.ErrorCode_E_OUTDATED_TERM:
		fmt.Printf("raft error: %+v, try again\n", fPart.GetCode().String())
		// just try again
	default:
		panic(fmt.Sprintf("unknown err code: %+v", fPart.GetCode().String()))
	}

	return resp, err
}

func (s *storageClientProxy) ChainAddEdges(req *storage.AddEdgesRequest) (*storage.ExecResponse, error) {
	resp, err := s.currLeader().ChainAddEdges(req)
	return s.checkResponse(resp, err)
}

func (s *storageClientProxy) markLeaderDown() {
	s.connDown[s.leader] = struct{}{}
}

func (s *storageClientProxy) updateLeader() {
	if len(s.connDown) == len(s.peers) {
		fmt.Printf("all peers down, wait 1s and will try aganin later")
		time.Sleep(1 * time.Second)
	}

	s.leader = (s.leader + 1) % len(s.peers)
	if _, ok := s.connDown[s.leader]; ok {
		// try reconnect if down
		if err := s.peers[s.leader].rest(); err != nil {
			delete(s.connDown, s.leader)
		} else {
			fmt.Printf("failed connecting %+v: %+v", s.peers[s.leader], err)
		}
	}
}

func toAddr(h *nebula.HostAddr) string {
	return fmt.Sprintf("%s:%d", h.Host, h.Port)
}

func NewStorageClient(m *MetaClient, opt StorageOption, spaceID nebula.GraphSpaceID, partID nebula.PartitionID) (StorageClient, error) {
	client := storageClientProxy{
		metaClient: m,
		connDown:   map[int]struct{}{},
	}

	listPartsReq := meta.ListPartsReq{
		SpaceID: spaceID,
		PartIds: []nebula.PartitionID{partID},
	}
	listPartResp, err := m.ListParts(&listPartsReq)
	if err != nil {
		return nil, fmt.Errorf("error creating storage client: %+v", err)
	}

	partItem := listPartResp.Parts[0]
	leaderAddr := toAddr(partItem.GetLeader())
	for i, h := range partItem.Peers {
		addr := toAddr(h)
		if addr == leaderAddr {
			client.leader = i
		}

		c, err := newDefaultStorageClient(addr, opt)
		if err != nil {
			return nil, fmt.Errorf("error creating storage client: %+v", err)
		}

		client.peers = append(client.peers, c)
	}

	return &client, nil
}
