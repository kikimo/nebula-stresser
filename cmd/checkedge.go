/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"math"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

type NebulaClient struct {
	metaClientIdx    int
	metaClients      []*meta.MetaServiceClient
	storageClientIdx int
	storageClients   []*storage.GraphStorageServiceClient
}

func newMetaClient(metaAddr string) (*meta.MetaServiceClient, error) {
	timeoutOption := thrift.SocketTimeout(10 * time.Second)
	bufferSize := 128 << 10
	frameMaxLength := uint32(math.MaxUint32)
	addressOption := thrift.SocketAddr(metaAddr)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return nil, fmt.Errorf("failed to create a net.Conn-backed Transport,: %s", err.Error())
	}
	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(bufferSize)
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

func NewNebulaClient(metaAddrs []string) (*NebulaClient, error) {
	nebulaClient := &NebulaClient{}
	for _, metaAddr := range metaAddrs {
		metaClient, err := newMetaClient(metaAddr)
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

func (c *NebulaClient) GetMetaClient() *meta.MetaServiceClient {
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
	if err != nil {
		return fmt.Errorf("failed list nebula cluster, nested error: %+v", err)
	}

	storageServers := listClusterResp.GetStorageServers()
	for _, ss := range storageServers {
		host := ss.Host
		storageAddr := fmt.Sprintf("%s:%d", host.GetHost(), host.GetPort())
		storageClient, err := initStorageClient(storageAddr)
		if err != nil {
			return fmt.Errorf("error init storage clients, nested error: %+v", err)
		}

		c.storageClients = append(c.storageClients, storageClient)
	}

	return nil
}

func initStorageClient(addr string) (*storage.GraphStorageServiceClient, error) {
	timeoutOption := thrift.SocketTimeout(10 * time.Second)
	bufferSize := 128 << 10
	frameMaxLength := uint32(math.MaxUint32)
	addressOption := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return nil, fmt.Errorf("failed to create a net.Conn-backed Transport,: %s", err.Error())
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(bufferSize)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	storageClient := storage.NewGraphStorageServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := storageClient.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %s", err.Error())
	}

	if !storageClient.IsOpen() {
		return nil, fmt.Errorf("transport is off")
	}

	return storageClient, nil
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

func (s *NebulaStresser) doGetEdges(spaceName string, edgeName string, reverse bool) (map[string]string, error) {
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

	fmt.Printf("total edges found: %d\n", totalEdges)
	return edges, nil

}

func (s *NebulaStresser) getEdges(spaceName string, edgeName string, reverse bool) (map[string]string, error) {
	return s.doGetEdges(spaceName, edgeName, reverse)
}

// a - b
func setDiff(a, b map[string]string) []string {
	ret := []string{}

	for k, _ := range a {
		if _, ok := b[k]; !ok {
			ret = append(ret, k)
		}
	}

	return ret
}

func setUnion(a, b map[string]string) []string {
	all := map[string]struct{}{}

	for k, _ := range a {
		all[k] = struct{}{}
	}

	for k, _ := range b {
		all[k] = struct{}{}
	}

	ret := []string{}
	for k, _ := range all {
		ret = append(ret, k)
	}

	return ret
}

func setInterset(a, b map[string]string) []string {
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
			fmt.Printf("%s\n", e)
		}
		fmt.Println()
	}

	missingBackwardEdges := setDiff(forwardEdges, backwarkEdges)
	if len(missingBackwardEdges) > 0 {
		fmt.Printf("%d missing backward edges:\n", len(missingBackwardEdges))
		for _, e := range missingBackwardEdges {
			fmt.Printf("%s\n", e)
		}
		fmt.Println()
	}

	semiNormalEdges := setInterset(backwarkEdges, forwardEdges)
	fmt.Printf("found %d semi-normal edges()\n", len(semiNormalEdges))
	for _, k := range semiNormalEdges {
		if backwarkEdges[k] == forwardEdges[k] {
			continue
		}

		fmt.Printf("prop mismatch in edge: %s, forward prop: %s, backward: %s\n", k, forwardEdges[k], backwarkEdges[k])
	}

	totalEdgeSet := map[string]struct{}{}
	for _, e := range totalEdges {
		totalEdgeSet[e] = struct{}{}
	}

	missingEdges := []string{}
	for i := 1; i <= vertexes; i++ {
		for j := 1; j <= vertexes; j++ {
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

func run() {
	// conn()
	// storageConn()
	space := "ttos_3p3r"
	metaAddrs := []string{"192.168.15.11:9559"}
	nebulaClient, err := NewNebulaClient(metaAddrs)
	if err != nil {
		panic(err)
	}

	stresser := NewNebulStresser(nebulaClient)
	edgeName := "known2"
	stresser.CheckEdges(space, edgeName, 128)
}

// checkedgeCmd represents the checkedge command
var checkedgeCmd = &cobra.Command{
	Use:   "checkedge",
	Short: "Check integrity of nebula edge",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		run()
	},
}

func init() {
	rootCmd.AddCommand(checkedgeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// checkedgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// checkedgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}