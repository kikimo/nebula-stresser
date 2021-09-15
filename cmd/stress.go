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

	"github.com/kikimo/nebula-stresser/cmd/edge"
	"github.com/spf13/cobra"
)

var (
	insertSpace      string
	insertServerAddr *[]string
	insertClientNum  int
	insertVertexNum  int
)

var defaultServers = []string{"192.168.15.11:9669", "192.168.15.11:9119", "192.168.15.11:9009"}

// stressCmd represents the stress command
var stressCmd = &cobra.Command{
	Use:   "stress",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("stress called")
		edge.RunInsertEdge(insertSpace, insertClientNum, insertVertexNum, *insertServerAddr)
	},
}

func init() {
	stressCmd.Flags().StringVarP(&insertSpace, "space", "s", "ttos_3p3r", "nebula we operate on")
	stressCmd.Flags().IntVarP(&insertClientNum, "client", "c", 128, "number of concurrent clients")
	stressCmd.Flags().IntVarP(&insertVertexNum, "vertex", "x", 64, "number of vertex in the space(will try inserting vertex^2 edges)")
	insertServerAddr = stressCmd.Flags().StringArrayP("addrs", "a", defaultServers, "graph server addr")
	rootCmd.AddCommand(stressCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// stressCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// stressCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
