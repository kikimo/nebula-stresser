package edge

import (
	"fmt"
	"time"

	"github.com/kikimo/nebula-stresser/pkg/client"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

type NebulaClient struct {
	metaClientIdx    int
	metaClients      []*client.MetaClient
	storageClientIdx int
	storageClients   []*storage.GraphStorageServiceClient
}

func NewNebulaClient(metaAddrs []string) (*NebulaClient, error) {
	nebulaClient := &NebulaClient{}
	metaOpt := client.MetaOption{
		Timeout:    1 * time.Second,
		BufferSize: 128 << 10,
	}
	for _, metaAddr := range metaAddrs {
		metaClient, err := client.NewMetaClient(metaAddr, metaOpt)
		if err != nil {
			return nil, fmt.Errorf("error creating nebula client, nested error: %+v", err)
		}

		nebulaClient.metaClients = append(nebulaClient.metaClients, metaClient)
	}

	if err := nebulaClient.initStorageClients(); err != nil {
		return nil, fmt.Errorf("error creating nebula client, nested error: %+v", err)
	}

	return nebulaClient, nil
}

func (c *NebulaClient) GetMetaClient() *client.MetaClient {
	// FIXME concurrent
	c.metaClientIdx = (c.metaClientIdx + 1) % len(c.metaClients)
	return c.metaClients[c.metaClientIdx]
}

func (c *NebulaClient) GetStorageClient() *storage.GraphStorageServiceClient {
	c.storageClientIdx = (c.storageClientIdx + 1) % len(c.storageClients)
	return c.storageClients[c.storageClientIdx]
}

func (c *NebulaClient) initStorageClients() error {
	metaClient := c.GetMetaClient()
	listClusterReq := &meta.ListClusterInfoReq{}
	listClusterResp, err := metaClient.ListCluster(listClusterReq)
	// fmt.Printf("list cluster resp: %+v, err: %+v\n", listClusterResp, err)
	if err != nil {
		return fmt.Errorf("failed list nebula cluster, nested error: %+v", err)
	}

	storageServers := listClusterResp.GetStorageServers()
	storageOpt := client.StorageOption{
		Timeout:    1 * time.Second,
		BufferSize: 128 << 10,
	}
	// fmt.Printf("storage servers: %+v\n", storageServers)
	for _, ss := range storageServers {
		host := ss.Host
		storageAddr := fmt.Sprintf("%s:%d", host.GetHost(), host.GetPort())
		storageClient, err := client.NewGraphStorageServiceClient(storageAddr, storageOpt)
		if err != nil {
			return fmt.Errorf("error init storage clients, nested error: %+v", err)
		}

		c.storageClients = append(c.storageClients, storageClient)
	}

	return nil
}

type NebulaStresser struct {
	nebulaClient *NebulaClient
}

func NewNebulStresser(nebulaClient *NebulaClient) *NebulaStresser {
	return &NebulaStresser{
		nebulaClient: nebulaClient,
	}
}

func (s *NebulaClient) GetSpace(spaceName string) (*meta.GetSpaceResp, error) {
	mclient := s.GetMetaClient()
	getSpaceReq := meta.GetSpaceReq{
		SpaceName: []byte(spaceName),
	}
	getSpaceResp, err := mclient.GetSpace(&getSpaceReq)
	if err != nil {
		return nil, fmt.Errorf("error getting space id, nested error: %+v", err)
	}

	return getSpaceResp, nil
}

func (s *NebulaClient) GetEdgeItem(spaceID nebula.GraphSpaceID, edgeName string) (*meta.EdgeItem, error) {
	mclient := s.GetMetaClient()
	listEegesReq := &meta.ListEdgesReq{
		SpaceID: spaceID,
	}
	listEdgeResp, err := mclient.ListEdges(listEegesReq)
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

type EdgeProps struct {
	idx string
	ts  *nebula.DateTime
}

func (s *NebulaStresser) doGetEdges(spaceName string, edgeName string, reverse bool) (map[string]*EdgeProps, error) {
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
	edges := map[string]*EdgeProps{}

	props := [][]byte{[]byte("_src"), []byte("_type"), []byte("_rank"), []byte("_dst"), []byte("idx"), []byte("ts")}
	// props := [][]byte{[]byte("_src"), []byte("_type"), []byte("_rank"), []byte("_dst"), []byte("idx")}
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
	// fmt.Printf("number of parts: %d, sclients: %+v\n", parts, s.nebulaClient.storageClients)
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
				// fmt.Printf("scan edge resp: %+v, err: %+v\n", scanEdgeResp, err)
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
					// TODO check data consistence
					ts := values[5].DtVal
					// fmt.Printf("ts: %+v\n", ts)
					p := EdgeProps{
						idx: idx,
						ts:  ts,
					}
					key := fmt.Sprintf("%d->%d", *src, *dst)
					edges[key] = &p
				}

				hasNext = scanEdgeResp.GetHasNext()
				cursor = scanEdgeResp.GetNextCursor()
			}
		}

	}

	return edges, nil
}

func (s *NebulaStresser) getEdges(spaceName string, edgeName string, reverse bool) (map[string]*EdgeProps, error) {
	return s.doGetEdges(spaceName, edgeName, reverse)
}

// a - b
func setDiff(a, b map[string]*EdgeProps) []string {
	ret := []string{}

	for k := range a {
		if _, ok := b[k]; !ok {
			ret = append(ret, k)
		}
	}

	return ret
}

func setUnion(a, b map[string]*EdgeProps) []string {
	all := map[string]struct{}{}

	for k := range a {
		all[k] = struct{}{}
	}

	for k := range b {
		all[k] = struct{}{}
	}

	ret := []string{}
	for k := range all {
		ret = append(ret, k)
	}

	return ret
}

func setInterset(a, b map[string]*EdgeProps) []string {
	ret := []string{}

	for k, _ := range a {
		if _, ok := b[k]; ok {
			ret = append(ret, k)
		}
	}

	return ret
}

func (s *NebulaStresser) CheckEdges(spaceName string, edge string, vertexes int) error {
	forwardEdges, err := s.getEdges(spaceName, edge, false)
	if err != nil {
		return fmt.Errorf("failed getting forward edges: %+v", err)
	}
	fmt.Printf("found %d forward edges\n", len(forwardEdges))

	backwarkEdges, err := s.getEdges(spaceName, edge, true)
	if err != nil {
		return fmt.Errorf("failed getting forward edges: %+v", err)
	}
	fmt.Printf("found %d backward edges\n", len(backwarkEdges))

	totalEdges := setUnion(forwardEdges, backwarkEdges)
	fmt.Printf("found %d edges in total\n", len(totalEdges))

	missingForwardEdges := setDiff(backwarkEdges, forwardEdges)
	if len(missingForwardEdges) > 0 {
		fmt.Printf("%d missing forward edges:\n", len(missingForwardEdges))
		for _, e := range missingForwardEdges {
			fmt.Printf("%s, corresponding backward edge prop: idx = %s, \n", e, backwarkEdges[e].idx)
		}
		fmt.Println()
	}

	missingBackwardEdges := setDiff(forwardEdges, backwarkEdges)
	if len(missingBackwardEdges) > 0 {
		fmt.Printf("%d missing backward edges:\n", len(missingBackwardEdges))
		for _, e := range missingBackwardEdges {
			fmt.Printf("%s, corresponding forward edge prop: idx = %s\n", e, forwardEdges[e].idx)
		}
		fmt.Println()
	}

	semiNormalEdges := setInterset(backwarkEdges, forwardEdges)
	fmt.Printf("found %d semi-normal edges\n", len(semiNormalEdges))
	for _, k := range semiNormalEdges {
		bp := backwarkEdges[k]
		fp := forwardEdges[k]

		// if bp.idx == fp.idx {
		if bp.idx == fp.idx && ((bp.ts == nil && fp.ts == nil) || (bp.ts != nil && fp.ts != nil && *bp.ts == *fp.ts)) {
			continue
		}

		toSign := func(eq bool) string {
			if eq {
				return "=="
			}

			return "!="
		}

		tsEq := bp.ts == fp.ts
		idxEq := bp.idx == fp.idx

		fmt.Printf("prop mismatch in edge %s, forward vs backward, idx: %s %s %s, ts: %+v %s %+v\n", k, fp.idx, toSign(idxEq), bp.idx, fp.ts, toSign(tsEq), bp.ts)
		// fmt.Printf("prop mismatch in edge: %s, forward prop: (idx  %s, ts %+v), backward: idx %s, ts %+v\n", k, fp.idx, fp.ts, bp.idx, bp.ts)
	}

	totalEdgeSet := map[string]struct{}{}
	for _, e := range totalEdges {
		totalEdgeSet[e] = struct{}{}
	}

	missingEdges := []string{}
	fmt.Printf("vertexes: %d\n", vertexes)
	for i := 0; i < vertexes; i++ {
		for j := 0; j < vertexes; j++ {
			k := fmt.Sprintf("%d->%d", i, j)
			if _, ok := totalEdgeSet[k]; !ok {
				missingEdges = append(missingEdges, k)
			}
		}
	}

	if len(missingEdges) > 0 {
		fmt.Printf("found %d missing edges:\n", len(missingEdges))
		for _, e := range missingEdges {
			fmt.Printf("%s\n", e)
		}
	}

	return nil
}

func (s *NebulaStresser) InitTestSpace(space string) {
	// TODO
}

func (s *NebulaClient) StressEdges(space string) {
	// TODO
}

func RunCheckEdge(spaceName string, edgeName string, vertexes int, metaAddr string) {
	metaAddrs := []string{metaAddr}
	nebulaClient, err := NewNebulaClient(metaAddrs)
	if err != nil {
		panic(err)
	}

	stresser := NewNebulStresser(nebulaClient)
	if err := stresser.CheckEdges(spaceName, edgeName, vertexes); err != nil {
		panic(fmt.Sprintf("failed checking edge: %+v", err))
	}
}
