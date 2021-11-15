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
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kikimo/nebula-stresser/pkg/client"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

var (
	edgePerfMetaAddr   string
	edgePerfClientNum  int
	edgePerfVertexNum  int
	edgePerfEnableToss bool
	edgePerfSpace      string
)

const (
	defaultEdgePerfMetaAddr = "192.168.15.12:9559"
	defaultEdgePerfSpace    = "ttos_3p3r"
	// defaultStressEdgeMetaAddr = "192.168.8.53:9559"
)

// edgePerfCmd represents the edgePerf command
var edgePerfCmd = &cobra.Command{
	Use:   "edgePerf",
	Short: "Edge perf test",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunEdgePerf(edgePerfClientNum, edgePerfVertexNum)
	},
}

func init() {
	edgePerfCmd.Flags().StringVarP(&edgePerfMetaAddr, "meta_addr", "m", defaultEdgePerfMetaAddr, "meta server addr")
	edgePerfCmd.Flags().IntVarP(&edgePerfClientNum, "client", "c", 1, "number of clients")
	edgePerfCmd.Flags().IntVarP(&edgePerfVertexNum, "vertex", "x", 1, "number of vertex")
	edgePerfCmd.Flags().BoolVarP(&edgePerfEnableToss, "toss", "t", true, "enable toss")
	edgePerfCmd.Flags().StringVarP(&edgePerfSpace, "space", "s", defaultEdgePerfSpace, "space to operate on")

	rootCmd.AddCommand(edgePerfCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// edgePerfCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// edgePerfCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func doEdgePerf(client client.StorageClient, spaceID nebula.GraphSpaceID, numParts int32, edgeType nebula.EdgeType, src int, dst int, idx string, rank nebula.EdgeRanking) (*storage.ExecResponse, error) {
	// src := uint64(1)
	// dst := uint64(1)

	srcData := [8]byte{}
	dstData := [8]byte{}
	binary.LittleEndian.PutUint64(srcData[:], uint64(src))
	binary.LittleEndian.PutUint64(dstData[:], uint64(dst))
	propIdx := &nebula.Value{
		SVal: []byte(idx),
	}
	props := []*nebula.Value{propIdx}
	// edgeType := nebula.EdgeType
	eKey := storage.EdgeKey{
		Src: &nebula.Value{
			SVal: srcData[:],
		},
		Dst: &nebula.Value{
			SVal: dstData[:],
		},
		EdgeType: edgeType,
		Ranking:  rank,
		// EdgeType: int32(5),
	}
	// fmt.Printf("key: %+v\n", eKey)
	edges := []*storage.NewEdge_{
		{
			Key:   &eKey,
			Props: props,
		},
	}
	partID := getPartID(int64(src), numParts)
	parts := map[nebula.PartitionID][]*storage.NewEdge_{
		int32(partID): edges,
	}
	req := storage.AddEdgesRequest{
		SpaceID: spaceID,
		// SpaceID:   8848,
		Parts: parts,
		// PropNames: [][]byte{[]byte("idx")},
	}

	// fmt.Printf("client: %+v, req: %+v\n", client, req)
	// FIXME use function pointer instead
	// if stressEdgeEnableToss {
	// 	return client.ChainAddEdges(&req)
	// } else {
	// 	return client.AddEdges(&req)
	// }
	return client.ChainAddEdges(&req)
	// return client.AddEdges(&req)
}

func RunEdgePerf(clientNum int, vertexNum int) {
	// clientNum := 1
	// vertexNum := 64
	// spaceID := 1
	spaceName := "ttos_3p3r"
	edgeName := "known2"

	metaOpt := client.MetaOption{
		Timeout:    8 * time.Second,
		BufferSize: 128 << 10, // FIXME magic number
	}
	fmt.Printf("meta addr: %+v\n", edgePerfMetaAddr)
	metaClient, err := client.NewMetaClient(edgePerfMetaAddr, metaOpt)
	if err != nil {
		panic(fmt.Sprintf("failed creating meta client: %+v", err))
	}

	spaceResp, err := metaClient.GetSpaceByName(spaceName)
	if err != nil {
		panic(err)
	}
	spaceID := spaceResp.GetItem().GetSpaceID()
	numParts := spaceResp.GetItem().Properties.GetPartitionNum()
	fmt.Printf("space id of %s is: %d\n", spaceName, spaceID)

	edgeItem, err := metaClient.GetEdgeItem(spaceID, edgeName)
	if err != nil {
		panic(err)
	}
	edgeType := edgeItem.GetEdgeType()
	fmt.Printf("edge type of %s is: %d\n", edgeName, edgeType)

	storageOpt := client.StorageOption{
		Timeout:    256 * time.Millisecond,
		BufferSize: 128 << 10, // FIXME magic number
	}
	clients := make([]client.StorageClient, clientNum)
	for i := range clients {
		mClient, err := client.NewMetaClient(edgePerfMetaAddr, metaOpt)
		if err != nil {
			panic(fmt.Sprintf("failed creating meta client: %+v", err))
		}

		clients[i], err = client.NewStorageClient(mClient, storageOpt, spaceID, numParts)
		if err != nil {
			panic(fmt.Sprintf("failed creating storage client: %+v", err))
		}
	}

	// TODO batch insert edge
	edges := vertexNum * vertexNum
	var waitClients sync.WaitGroup
	waitClients.Add(clientNum)
	var (
		totalReq  int64 = 0
		failedReq int64 = 0
	)
	// resultStats := make([][]bool, clientNum)
	for i := 0; i < clientNum; i++ {
		// resultStats[i] = make([]bool, edges)
		// go func(cid int, stats []bool) {
		go func(cid int) {
			// for j := 0; j <
			rank := nebula.EdgeRanking(cid)
			for j := 0; j < edges; j++ {
				src := j / vertexNum
				dst := j % vertexNum
				idx := fmt.Sprintf("%d->%d", cid, j)
				// fmt.Printf("client %d insert %d edge\n", cid, j)
				// retry := 0
				for {
					// resp, err := doStressEdge(clients[cid], spaceID, numParts, edgeType, src, dst, idx)
					// start := time.Now()
					resp, err := doEdgePerf(clients[cid], spaceID, numParts, edgeType, src, dst, idx, rank)
					// _, err := doEdgePerf(clients[cid], spaceID, numParts, edgeType, src, dst, idx, rank)
					// dur := time.Since(start)
					// fmt.Printf("dur: %d\n", dur.Milliseconds())
					atomic.AddInt64(&totalReq, 1)

					if err == nil {
						// resultStats[cid][j] = true
						break
					}

					// ignore nebula error
					// if strings.Contains(err.Error(), "nebula error: ") {
					fmt.Printf("error insert edge: %+v, resp: %+v, retry\n", err, resp)
					atomic.AddInt64(&failedReq, 1)
					// 	break
					// }
					break

					// fmt.Printf("error insert edge: %+v, resp: %+v, retry\n", err, resp)
					// retry++
				}

			}
			waitClients.Done()
		}(i)
	}

	waitClients.Wait()
	fmt.Printf("total req: %d\n", atomic.LoadInt64(&totalReq))
	fmt.Printf("failed req: %d\n", atomic.LoadInt64(&failedReq))
	fmt.Printf("done inserting edge\n")
}
