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
	"time"

	"github.com/kikimo/nebula-stresser/pkg/client"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

var (
	stressEdgeMetaAddr   string
	stressEdgeClientNum  int
	stressEdgeVertexNum  int
	stressEdgeEnableToss bool
)

const (
	defaultStressEdgeMetaAddr = "192.168.15.11:8448"
	// defaultStressEdgeMetaAddr = "192.168.8.53:9559"
)

// stressEdgeCmd represents the stressEdge command
var stressEdgeCmd = &cobra.Command{
	Use:   "stressEdge",
	Short: "stree test fo edge inserting",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunStressEdge(stressEdgeClientNum, stressEdgeVertexNum)
	},
}

func init() {
	stressEdgeCmd.Flags().StringVarP(&stressEdgeMetaAddr, "meta_addr", "m", defaultStressEdgeMetaAddr, "meta server addr")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeClientNum, "client", "c", 1, "number of clients")
	stressEdgeCmd.Flags().IntVarP(&stressEdgeVertexNum, "vertex", "x", 1, "number of vertex")
	stressEdgeCmd.Flags().BoolVarP(&stressEdgeEnableToss, "toss", "t", true, "enable toss")
	rootCmd.AddCommand(stressEdgeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// stressEdgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// stressEdgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func getPartID(vid int64, numParts int32) int32 {
	return int32(vid%int64(numParts)) + 1
}

func doStressEdge(client client.StorageClient, spaceID nebula.GraphSpaceID, numParts int32, edgeType nebula.EdgeType, src int, dst int, idx string) (*storage.ExecResponse, error) {
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
		Parts:     parts,
		PropNames: [][]byte{[]byte("idx")},
	}

	// fmt.Printf("client: %+v, req: %+v\n", client, req)
	// FIXME use function pointer instead
	if stressEdgeEnableToss {
		return client.ChainAddEdges(&req)
	} else {
		return client.AddEdges(&req)
	}
}

func RunStressEdge(clientNum int, vertexNum int) {
	// clientNum := 1
	// vertexNum := 64
	// spaceID := 1
	shuffleWindow := 1
	spaceName := "ttos_3p3r"
	edgeName := "known2"

	metaOpt := client.MetaOption{
		Timeout:    8 * time.Second,
		BufferSize: 128 << 10, // FIXME magic number
	}
	fmt.Printf("meta addr: %+v\n", stressEdgeMetaAddr)
	metaClient, err := client.NewMetaClient(stressEdgeMetaAddr, metaOpt)
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
		mClient, err := client.NewMetaClient(stressEdgeMetaAddr, metaOpt)
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
	// for i := range stmts {
	// 	stmts[i] = make([]string, edges)
	// 	// 500 edges
	// 	for j := 0; j < edges; j++ {
	// 		src, dst := j+1, j+edges+1
	// 		stmts[i][j] = fmt.Sprintf(`insert edge known2(idx) values %d->%d:("%d-%d")`, src, dst, i+1, j+1)
	// 		// fmt.Printf("%s\n", stmts[i][j])
	// 	}
	// }
	if shuffleWindow > 1 {
		// for i := 0; i < edges; i += shuffleWindow {
		// 	// shuffle edge through [i, i + shuffleWindow)
		// 	start, end := i, i+shuffleWindow
		// 	if end > edges {
		// 		end = edges
		// 	}

		// 	// TODO perform shuffle
		// 	sz := end - start
		// 	for j := 0; j < clientNum; j++ {
		// 		rand.Shuffle(sz, func(x, y int) {
		// 			stmts[j][x+i], stmts[j][y+i] = stmts[j][y+i], stmts[j][x+i]
		// 		})
		// 	}
		// }
		fmt.Printf("skip shuffle\n")
	}

	// for i := 0; i < edges; i++ {
	// 	fmt.Printf("%d: %s\n", i+1, stmts[0][i])
	// }
	// metaLock := sync.Mutex{}
	fmt.Printf("inserting edge...\n")
	var waitClients sync.WaitGroup
	waitClients.Add(clientNum)
	for i := 0; i < clientNum; i++ {
		go func(cid int) {
			// for j := 0; j <
			for j := 0; j < edges; j++ {
				src := j / vertexNum
				dst := j % vertexNum
				idx := fmt.Sprintf("%d->%d", cid, j)
				fmt.Printf("client %d insert %d edge\n", cid, j)
				retry := 0
				for {
					resp, err := doStressEdge(clients[cid], spaceID, numParts, edgeType, src, dst, idx)
					if err == nil {
						break
					}

					fmt.Printf("error insert edge: %+v, resp: %+v", err, resp)
					retry++
				}

			}
			waitClients.Done()
		}(i)
	}
	waitClients.Wait()

	/*
		for i := 0; i < edges; i++ {
			fmt.Printf("insert edge %d\n", i)
			var wg sync.WaitGroup
			wg.Add(clientNum)
			for j := range clients {
				go func(cid int, edge int) {
					src := edge / vertexNum
					dst := edge % vertexNum
					idx := fmt.Sprintf("%d->%d", cid, edge)
					for {
						_, err := doStressEdge(clients[cid], spaceID, edgeType, src, dst, idx)
						if err != nil {
							if strings.Index(err.Error(), "wrong leader") == 0 {
								// panic(fmt.Sprintf("fuck wrong leader: %+v", err))
								fmt.Printf("%d wrong leader %d, try again inserting %d, err: %+v\n", cid, clients[cid].Leader(), edge, err)
							} else if strings.Index(err.Error(), "write tcp ") == 0 || strings.Index(err.Error(), "read tcp ") == 0 {
								fmt.Printf("error inserting edge %d: %+v\n", edge, err)
								// metaLock.Lock()
								// clients[cid], err = client.NewStorageClient(metaClient, storageOpt)
								// metaLock.Unlock()
								// if err != nil {
								// 	panic(err)
								// }

								clients[cid].UpdateLeader(nil)
							} else {
								fmt.Printf("fuck error inserting edge %d: %+v\n", edge, err)
								metaLock.Lock()
								clients[cid], err = client.NewStorageClient(metaClient, storageOpt)
								metaLock.Unlock()
								if err != nil {
									panic(err)
								}
							}
						} else {
							break
						}
					}
					wg.Done()
				}(j, i)
			}
			wg.Wait()
		}
	*/

	// for i := 0; i < edges; i++ {
	// 	fmt.Printf("insert edge %d\n", i)
	// 	var wg sync.WaitGroup
	// 	wg.Add(clientNum)
	// 	for j := range clients {
	// 		go func(cid int, c *client.StorageClient, edge int) {
	// 			src := edge / vertexNum
	// 			dst := edge % vertexNum
	// 			idx := fmt.Sprintf("%d->%d", cid, edge)
	// 			for {
	// 				_, err := doStressEdge(c, spaceID, edgeType, src, dst, idx)
	// 				if err != nil {
	// 					if strings.Index(err.Error(), "wrong leader") == 0 {
	// 						// panic(fmt.Sprintf("fuck wrong leader: %+v", err))
	// 						fmt.Printf("%d wrong leader %d, try again inserting %d, err: %+v\n", cid, c.Leader(), edge, err)
	// 					} else if strings.Index(err.Error(), "write tcp ") == 0 || strings.Index(err.Error(), "read tcp ") == 0 {
	// 						fmt.Printf("error inserting edge: %+v\n", err)
	// 						clients[cid], err = client.NewStorageClient(metaClient, storageOpt)
	// 						if err != nil {
	// 							panic(err)
	// 						}
	// 						// clients[cid].UpdateLeader()
	// 					} else {
	// 						fmt.Printf("fuck error inserting edge: %+v\n", err)
	// 					}
	// 				} else {
	// 					break
	// 				}
	// 			}
	// 			wg.Done()
	// 		}(j, clients[j], i)
	// 	}
	// 	wg.Wait()
	// }
	fmt.Printf("done inserting edge\n")

	/*
			failed := 0
			for i := range doneSet {
				if doneSet[i] == 0 {
					in := i / vertexNum
					out := i % vertexNum
					fmt.Printf("failed inserting %d->%d\n", in, out)
					failed++
				}
			}

		fmt.Printf("failed: %d\n", failed)
	*/

	// TODO don't delete me
	// for i := range clients {
	// 	go func(c *Client, stmt []string) {
	// 		// fmt.Printf("session: %+v\n", c.session)
	// 		for _, stm := range stmt {
	// 			for k := 0; k < 100; k++ {
	// 				rs, err := c.session.Execute(stm)
	// 				if err != nil {
	// 					panic(fmt.Sprintf("failed inserting edge: %s", stm))
	// 				}

	// 				checkResultSet("", rs)
	// 			}
	// 		}
	// 		wg.Done()
	// 	}(&clients[i], stmts[i])
	// }

	// wg.Wait()
	// fmt.Printf("insert done!\n")

	// for _, c := range clients {
	// 	c.Release()
	// }

}
