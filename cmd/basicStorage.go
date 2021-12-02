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
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

var rateLimit int

// basicStorageCmd represents the basicStorage command
var basicStorageCmd = &cobra.Command{
	Use:   "basicStorage",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunBasicStorag()
	},
}

func NewClient(addr string) (*storage.GraphStorageServiceClient, error) {
	timeout := thrift.SocketTimeout(4 * time.Second)
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		return nil, fmt.Errorf("failed creating a net.Conn-backed Transport,: %+v", err)
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(65536)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	client := storage.NewGraphStorageServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		return nil, fmt.Errorf("failed to open transport, error: %+v", err)
	}

	if !client.IsOpen() {
		panic("transport is off")
	}

	return client, nil
}

var hostAddrs = []string{
	"192.168.15.12:54961",
	"192.168.15.12:34715",
	"192.168.15.12:50293",
	"192.168.15.12:37873",
	"192.168.15.12:40175",
	"192.168.15.12:39491",
	"192.168.15.12:38713",
}

type NebulaClient struct {
	id         int
	client     *storage.GraphStorageServiceClient
	nextLeader int
	hostAddrs  []string
}

func NewNebulaClient(id int, hostAddrs []string) *NebulaClient {
	c := NebulaClient{
		id:        id,
		hostAddrs: hostAddrs,
	}

	return &c
}

func (c *NebulaClient) SetLeader(addr string) error {
	found := false
	for i := range c.hostAddrs {
		if c.hostAddrs[i] == addr {
			c.nextLeader = i
			found = true
		}
	}

	if !found {
		c.hostAddrs = append(c.hostAddrs, addr)
		c.nextLeader = len(c.hostAddrs) - 1
	}

	return c.RestConn()
}

func (c *NebulaClient) RestConn() error {
	addr := c.hostAddrs[c.nextLeader]
	c.nextLeader = (c.nextLeader + 1) % len(c.hostAddrs)
	client, err := NewClient(addr)
	if err != nil {
		return err
	}

	c.client = client
	return nil
}

func RunBasicStorag() {
	/*
		// addr := "192.168.15.12:46153"
		clients := []*NebulaClient{}
		totalClients := 1024
		for i := 0; i < totalClients; i++ {
			client := NewNebulaClient(i, hostAddrs)
			clients = append(clients, client)
			client.RestConn()
		}

		limit := rate.Every(time.Microsecond * time.Duration(rateLimit))
		limiter := rate.NewLimiter(limit, 1024)
		ctx := context.TODO()

		loops := 6553600
		var wg sync.WaitGroup
		wg.Add(len(clients))
		fmt.Printf("putting kvs...\n")
		for i := range clients {
			// go func(id int, client *storage.GraphStorageServiceClient) {
			go func(id int) {
				client := clients[id]
				for j := 0; j < loops; j++ {
					key := fmt.Sprintf("%d-key1-%d", id, j)
					value := fmt.Sprintf("%d-value1-%d", id, j)
					putReq := storage.KVPutRequest{
						SpaceID: 1,
						Parts: map[int32][]*nebula.KeyValue{
							1: {
								{
									Key:   []byte(key),
									Value: []byte(value),
								},
							},
						},
					}

					if client.client == nil {
						client.RestConn()
					}

					if client.client == nil {
						continue
					}

					limiter.Wait(ctx)
					putResp, err := client.client.Put(&putReq)
					if err != nil {
						// panic(err)
						if strings.Contains(err.Error(), "i/o timeout") {
							client.RestConn()
						} else if strings.Contains(err.Error(), "Invalid data length") {
							client.RestConn()
						} else if strings.Contains(err.Error(), "Not enough frame size") {
							client.RestConn()
						} else if strings.Contains(err.Error(), "put failed: out of sequence response") {
							client.RestConn()
						} else if strings.Contains(err.Error(), "Bad version in") {
							client.RestConn()
						} else if strings.Contains(err.Error(), "broken pipe") {
							client.RestConn()
						} else {
							fmt.Printf("fuck: %+v\n", err)
						}

						continue
					}

					if len(putResp.Result_.FailedParts) == 0 {
						// fmt.Println(putResp)
						// getReq := storage.KVGetRequest{
						// 	SpaceID: 1,
						// 	Parts: map[int32][][]byte{
						// 		1: {
						// 			[]byte(key),
						// 		},
						// 	},
						// }

						// getResp, err := client.Get(&getReq)
						// if err != nil {
						// 	panic(err)
						// }

						// kvs := getResp.GetKeyValues()
						// for k := range kvs {
						// 	fmt.Printf("key: %s, value: %s\n", k, string(kvs[k]))
						// }
					} else {
						fpart := putResp.Result_.FailedParts[0]
						fmt.Println(fpart)
						switch fpart.Code {
						case nebula.ErrorCode_E_LEADER_CHANGED:
							if fpart.Leader != nil {
								leaderAddr := fmt.Sprintf("%s:%d", fpart.Leader.Host, fpart.Leader.Port)
								fmt.Printf("connecting to leader %s for client %d\n", leaderAddr, id)
								client.SetLeader(leaderAddr)
								// clients[id] = NewClient(leaderAddr)
								// client = clients[id]
							}
						case nebula.ErrorCode_E_CONSENSUS_ERROR:
							// ignore
						}
					}
				}
				wg.Done()
			}(i)
			// }(i, clients[i])

			// fmt.Println(getResp)
		}

		wg.Wait()
		fmt.Printf("done putting kvs...\n")
	*/
}

func init() {
	rootCmd.AddCommand(basicStorageCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// basicStorageCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// basicStorageCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	basicStorageCmd.Flags().IntVarP(&rateLimit, "limit", "l", 100, "rate limit, every query per limit us")
}
