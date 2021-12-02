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
	"github.com/vesoft-inc/nebula-go/v2/raftex"
)

var (
	raftSpace = 1
	raftPart  = 1
)

// raftCmd represents the raft command
var raftCmd = &cobra.Command{
	Use:   "raft",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("getting raft state:")
		RunRaft()
	},
}

func RunRaft() {
	addr := "127.0.0.1:9780"
	timeout := thrift.SocketTimeout(4 * time.Second)
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr(addr)
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		// return nil, fmt.Errorf("failed creating a net.Conn-backed Transport,: %+v", err)
		panic(fmt.Sprintf("failed creating a net.Conn-backed Transport,: %+v", err))
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(65536)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()

	client := raftex.NewRaftexServiceClientFactory(transport, pf)
	if err := client.Open(); err != nil {
		panic(err)
	}

	if !client.IsOpen() {
		panic("transport is off")
	}

	req := raftex.GetStateRequest{
		Space: int32(raftSpace),
		Part:  int32(raftPart),
	}
	resp, err := client.GetState(&req)
	if err != nil {
		panic(err)
	}

	fmt.Printf("resp: %+v\n", resp)
}

func init() {
	rootCmd.AddCommand(raftCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// raftCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// raftCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	raftCmd.Flags().IntVarP(&raftSpace, "space", "s", 1, "specify raft space")
	raftCmd.Flags().IntVarP(&raftPart, "part", "p", 1, "specify raft part")
}
