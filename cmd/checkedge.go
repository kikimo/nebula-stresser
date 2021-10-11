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
	"github.com/kikimo/nebula-stresser/cmd/edge"
	"github.com/spf13/cobra"
)

var (
	checkSpace         string
	checkEdge          string
	checkMetaAddr      string
	checkEdgeVertexNum int
)

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
		edge.RunCheckEdge(checkSpace, checkEdge, checkEdgeVertexNum, checkMetaAddr)
	},
}

func init() {
	// checkedgeCmd.Flags().StringVarP(&)
	checkedgeCmd.Flags().StringVarP(&checkSpace, "space", "s", "ttos_3p3r", "nebula to check")
	checkedgeCmd.Flags().StringVarP(&checkEdge, "edge", "e", "known2", "edge to check")
	checkedgeCmd.Flags().IntVarP(&checkEdgeVertexNum, "vertex", "x", 32, "number of vertex we wanna check")
	checkedgeCmd.Flags().StringVarP(&checkMetaAddr, "meta_addr", "m", "192.168.15.11:8448", "meta server addr")
	rootCmd.AddCommand(checkedgeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// checkedgeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// checkedgeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
