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

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/spf13/cobra"
	"github.com/vesoft-inc/nebula-go/v2/nebula"
	"github.com/vesoft-inc/nebula-go/v2/nebula/storage"
)

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

func RunBasicStorag() {
	timeout := thrift.SocketTimeout(0)
	// TODO what for?
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr("192.168.15.12:46153")
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		panic(fmt.Sprintf("failed creating a net.Conn-backed Transport,: %+v", err))
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(65536)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	client := storage.NewGraphStorageServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		panic(fmt.Sprintf("failed to open transport, error: %+v", err))
	}

	if !client.IsOpen() {
		panic("transport is off")
	}

	putReq := storage.KVPutRequest{
		SpaceID: 1,
		Parts: map[int32][]*nebula.KeyValue{
			1: {
				{
					Key:   []byte("hello"),
					Value: []byte("world"),
				},
			},
		},
	}

	putResp, err := client.Put(&putReq)
	if err != nil {
		panic(err)
	}
	fmt.Println(putResp)

	getReq := storage.KVGetRequest{
		SpaceID: 1,
		Parts: map[int32][][]byte{
			1: {
				[]byte("hello"),
			},
		},
	}

	getResp, err := client.Get(&getReq)
	if err != nil {
		panic(err)
	}

	kvs := getResp.GetKeyValues()
	for k := range kvs {
		fmt.Printf("key: %s, value: %s\n", k, string(kvs[k]))
	}

	// fmt.Println(getResp)
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
}
