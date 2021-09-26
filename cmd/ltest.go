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
	"github.com/kikimo/nebula-stresser/cmd/ltest"
	"github.com/spf13/cobra"
)

var (
	ltestSpaceName  string
	ltestEdgeName   string
	ltestClients    int
	ltestIterCount  int
	ltestVertexes   int
	ltestServerAddr *[]string
)

// TODO move to global flags
var defaultLtestServers = []string{"192.168.15.11:9669", "192.168.15.11:9119", "192.168.15.11:9009"}

// ltestCmd represents the ltest command
var ltestCmd = &cobra.Command{
	Use:   "ltest",
	Short: "Linearizability test using Procupine",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		ltest.RunLinearizabilityTestAndCheck(ltestSpaceName, ltestEdgeName, ltestClients, ltestIterCount, ltestVertexes, *ltestServerAddr)
	},
}

func init() {
	ltestCmd.Flags().StringVarP(&ltestSpaceName, "space", "s", "ltest_0", "space to operate on")
	ltestCmd.Flags().StringVarP(&ltestEdgeName, "edge", "e", "ledge_0", "edge to operate on")
	ltestCmd.Flags().IntVarP(&ltestClients, "clients", "c", 4, "number of concurrents")
	ltestCmd.Flags().IntVarP(&ltestIterCount, "iter", "i", 512, "number of iterations")
	ltestCmd.Flags().IntVarP(&ltestVertexes, "vertexes", "x", 2, "number of vertexes")
	ltestServerAddr = ltestCmd.Flags().StringArrayP("addrs", "a", defaultLtestServers, "graph server addr")
	rootCmd.AddCommand(ltestCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// ltestCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// ltestCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
