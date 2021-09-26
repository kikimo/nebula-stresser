/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package edge

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/kikimo/nebula-stresser/pkg/client"
	nebula "github.com/vesoft-inc/nebula-go/v2"
)

const (
	address = "192.168.15.11"
	// The default port of Nebula Graph 2.x is 9669.
	// 3699 is only for testing.
	port     = 9669
	username = "root"
	password = "nebula"
)

// Initialize logger
var log = nebula.DefaultLogger{}

// type Client struct {
// 	ID      int
// 	session *nebula.Session
// }

func convertAddrs(addrs []string) ([]nebula.HostAddress, error) {
	hostAddrs := []nebula.HostAddress{}
	for _, a := range addrs {
		parts := strings.Split(a, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("illegal graph address: %s", a)
		}

		port, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("illegal graph address: %s", a)
		}

		addr := nebula.HostAddress{
			Host: parts[0],
			Port: port,
		}

		hostAddrs = append(hostAddrs, addr)
	}

	return hostAddrs, nil
}

func RunInsertEdge(spaceName string, edgeName string, clientNum int, vertexNum int, addrs []string, shuffleWindow int) {
	doneSet := make([]int32, vertexNum*vertexNum)
	hostList, err := convertAddrs(addrs)
	if err != nil {
		panic(err)
	}

	// Create configs for connection pool using default values
	testPoolConfig := nebula.GetDefaultConf()
	testPoolConfig.MaxConnPoolSize = clientNum

	// Initialize connection pool
	pool, err := nebula.NewConnectionPool(hostList, testPoolConfig, log)
	if err != nil {
		log.Fatal(fmt.Sprintf("Fail to initialize the connection pool, host: %s, port: %d, %s", address, port, err.Error()))
	}
	// Close all connections in the pool
	defer pool.Close()

	clients := make([]*client.SessionX, clientNum)
	for i := range clients {
		session, err := pool.GetSession(username, password)
		if err != nil {
			panic(fmt.Sprintf("failed creating nebula session %+v", err))
		}
		clients[i] = client.New(i+1, session)

		if _, err := clients[i].Execute(fmt.Sprintf("use %s;", spaceName)); err != nil {
			panic(fmt.Sprintf("error switching space: %+v", err))
		}
	}

	// TODO batch insert edge
	edges := vertexNum * vertexNum
	fmt.Printf("building insert edge statments for clients...\n")
	stmts := make([][]string, clientNum)
	var swg sync.WaitGroup
	swg.Add(clientNum)
	for i := 0; i < clientNum; i++ {
		go func(iClient int) {
			iStmts := make([]string, edges)
			ith := 0

			for x := 0; x < vertexNum; x++ {
				for y := 0; y < vertexNum; y++ {
					stmt := fmt.Sprintf(`insert edge %s(idx) values %d->%d:("%d-%d")`, edgeName, x+1, y+1, iClient+1, ith+1)
					iStmts[ith] = stmt
					ith++
				}
			}
			stmts[iClient] = iStmts
			swg.Done()
		}(i)
	}
	swg.Wait()
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

	fmt.Printf("done building insert edge statments, inserting edges....\n")
	for i := 0; i < edges; i++ {
		var wg sync.WaitGroup
		wg.Add(clientNum)
		for j := range clients {
			go func(c *client.SessionX, stmt string) {
				// for k := 0; k < 100; k++ {
				for {
					_, err := c.Execute(stmt)
					if err == nil {
						fmt.Printf("done edge %d\n", i)
						atomic.StoreInt32(&doneSet[i], 1)
						break
					} else {
						// log.Error(fmt.Sprintf("client %d failed executing %s, %+v, retry...", c.GetID(), stmt, err))
						break
					}
				}
				// c.session.Execute(stmt)
				// fmt.Println(stmt)
				// if err != nil {
				// 	continue
				// }

				// if err := checkResultSet("", rs); err != nil {
				// 	continue
				// }

				// }
				wg.Done()
				// break
			}(clients[j], stmts[j][i])
		}
		wg.Wait()
	}

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
	fmt.Printf("insert done!\n")

	for _, c := range clients {
		c.Release()
	}
}
