package client

import (
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

type MetaOption struct {
	Timeout    time.Duration
	BufferSize int
}

type HostAddrX struct {
	host       *nebula.HostAddr
	ts         time.Time
	metaClient *MetaClient
	currLeader int
	peers      []*nebula.HostAddr
}

type MetaClient struct {
	*meta.MetaServiceClient
	lock            Spinlock
	spacePartLeader map[nebula.GraphSpaceID]map[nebula.PartitionID]*HostAddrX
}

func NewMetaClient(addr string, opt MetaOption) (*MetaClient, error) {
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

	client := &MetaClient{
		MetaServiceClient: metaClient,
		spacePartLeader:   map[int32]map[int32]*HostAddrX{},
	}

	return client, nil
}

func (m *MetaClient) GetSpaceByName(spaceName string) (*meta.GetSpaceResp, error) {
	getSpaceReq := meta.GetSpaceReq{
		SpaceName: []byte(spaceName),
	}
	getSpaceResp, err := m.GetSpace(&getSpaceReq)
	if err != nil {
		return nil, fmt.Errorf("error getting space id, nested error: %+v", err)
	}
	getSpaceResp.GetItem().Properties.GetPartitionNum()

	return getSpaceResp, nil
}

func (m *MetaClient) GetEdgeItem(spaceID nebula.GraphSpaceID, edgeName string) (*meta.EdgeItem, error) {
	listEegesReq := &meta.ListEdgesReq{
		SpaceID: spaceID,
	}
	listEdgeResp, err := m.ListEdges(listEegesReq)
	if err != nil {
		return nil, fmt.Errorf("error list space edge: %+v", err)
	}

	for _, er := range listEdgeResp.Edges {
		if string(er.EdgeName) == edgeName {
			return er, nil
		}
	}

	return nil, fmt.Errorf("edge %s not found", edgeName)
}

func (m *MetaClient) GetSpacePartLeader(spaceID nebula.GraphSpaceID, partID nebula.PartitionID) (*nebula.HostAddr, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if _, ok := m.spacePartLeader[spaceID]; !ok {
		m.spacePartLeader[spaceID] = map[nebula.PartitionID]*HostAddrX{}
	}

	partLeaderMap := m.spacePartLeader[spaceID]
	var hostx *HostAddrX
	var err error
	if _, ok := partLeaderMap[partID]; !ok {
		// partLeaderMap[partID] =
		hostx, err = m.doGetSpacePartLeader(spaceID, partID)
		if err != nil {
			return nil, err
		}

		partLeaderMap[partID] = hostx
	} else {
		hostx = partLeaderMap[partID]
		hostx.currLeader = (hostx.currLeader + 1) % len(hostx.peers)
		hostx.host = hostx.peers[hostx.currLeader]
	}

	hostx = partLeaderMap[partID]
	return hostx.host, nil
}

func (m *MetaClient) doGetSpacePartLeader(spaceID nebula.GraphSpaceID, partID nebula.PartitionID) (*HostAddrX, error) {
	lpReq := meta.ListPartsReq{
		SpaceID: spaceID,
		PartIds: []nebula.PartitionID{partID},
	}

	partResp, err := m.ListParts(&lpReq)
	if err != nil {
		return nil, err
	}

	part := partResp.Parts[0]
	peers := part.GetPeers()
	// fmt.Printf("parts of part %d at space %d is: %+v and the leader %+v", partID, spaceID, part, part.GetLeader())
	leader := part.GetLeader()
	hostx := &HostAddrX{
		host:       leader,
		peers:      peers,
		ts:         time.Now(),
		metaClient: m,
	}

	for idx, h := range peers {
		if h.GetPort() == leader.Port && h.GetHost() == leader.Host {
			hostx.currLeader = idx
		}
	}

	return hostx, nil
}

/*
func (m *MetaClient) doGetEdges(spaceName string, edgeName string, reverse bool) (map[string]string, error) {
	// TODO close client
	space, err := s.nebulaClient.GetSpace(spaceName)
	if err != nil {
		return nil, fmt.Errorf("error checking edges, nested error: %+v", err)
	}
	spaceID := space.Item.SpaceID
	parts := space.Item.Properties.PartitionNum
	// storageClient := s.nebulaClient.GetStorageClient()
	edgeItem, err := s.nebulaClient.GetEdgeItem(spaceID, edgeName)
	if err != nil {
		return nil, fmt.Errorf("error get edge: %+v", err)
	}
	edges := map[string]string{}

	props := [][]byte{[]byte("_src"), []byte("_type"), []byte("_rank"), []byte("_dst"), []byte("idx")}
	if reverse {
		props[0], props[3] = props[3], props[0]
	}

	edgeType := edgeItem.EdgeType
	if reverse {
		edgeType = -edgeType
	}

	edgeProps := &storage.EdgeProp{
		Type:  edgeType,
		Props: props,
	}

	totalEdges := 0
	for _, sclient := range s.nebulaClient.storageClients {
		for i := 1; i <= int(parts); i++ {
			hasNext := true
			var cursor []byte = nil
			for hasNext {
				scanEdgeRequest := storage.ScanEdgeRequest{
					SpaceID:       spaceID,
					PartID:        int32(i),
					Limit:         1024,
					Cursor:        cursor,
					ReturnColumns: edgeProps,
				}
				scanEdgeResp, err := sclient.ScanEdge(&scanEdgeRequest)
				if err != nil {
					return nil, fmt.Errorf("error scanning edge: %+v", err)
				}
				totalEdges += len(scanEdgeResp.EdgeData.Rows)
				// fmt.Printf("scan edge: %+v\n", scanEdgeResp)

				edgeData := scanEdgeResp.EdgeData
				rows := edgeData.Rows
				for _, row := range rows {
					values := row.Values
					// fmt.Printf("val size: %d\n", len(values))
					// fmt.Printf("vals: %+v\n", values)
					src, dst := values[0].IVal, values[3].IVal
					idx := string(values[4].SVal)
					key := fmt.Sprintf("%d->%d", *src, *dst)
					edges[key] = idx
				}

				hasNext = scanEdgeResp.GetHasNext()
				cursor = scanEdgeResp.GetNextCursor()
			}
		}

	}

	return edges, nil
}

func (s *NebulaStresser) getEdges(spaceName string, edgeName string, reverse bool) (map[string]string, error) {
	return s.doGetEdges(spaceName, edgeName, reverse)
}
*/
