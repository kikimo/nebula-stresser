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
	"github.com/vesoft-inc/nebula-go/v2/nebula/meta"
)

// changeLeaderCmd represents the changeLeader command
var changeLeaderCmd = &cobra.Command{
	Use:   "changeLeader",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("changeLeader called")
		RunChangeLeader()
	},
}

func RunChangeLeader() {
	timeout := thrift.SocketTimeout(250 * time.Millisecond)
	// bufferSize := 128 << 10
	frameMaxLength := uint32(math.MaxUint32)
	sockAddr := thrift.SocketAddr("192.168.15.11:9559")
	sock, err := thrift.NewSocket(timeout, sockAddr)
	if err != nil {
		panic(fmt.Sprintf("failed to create a net.Conn-backed Transport,: %s", err.Error()))
	}

	// Set transport buffer
	bufferedTranFactory := thrift.NewBufferedTransportFactory(4096 * 1024)
	transport := thrift.NewFramedTransportMaxLength(bufferedTranFactory.GetTransport(sock), frameMaxLength)
	pf := thrift.NewBinaryProtocolFactoryDefault()
	metaClient := meta.NewMetaServiceClientFactory(transport, pf)
	// cn.graph = graph.NewGraphServiceClientFactory(transport, pf)
	if err := metaClient.Open(); err != nil {
		panic(fmt.Sprintf("failed to open transport, error: %s", err.Error()))
	}

	if !metaClient.IsOpen() {
		panic("transport is off")
	}

	fmt.Println((metaClient))
	// now
	// 1. list part of spaces
	// 2. make connection to it
}

func init() {
	rootCmd.AddCommand(changeLeaderCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// changeLeaderCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// changeLeaderCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
