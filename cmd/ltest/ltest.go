/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
package ltest

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
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

type Client struct {
	ID      int
	session *nebula.Session
}

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

func RunLinearizabilityTestAndCheck(spaceName string, edgeName string, clientNum int, iterCount int, vertexes int, addrs []string) {
	// 1. create space if no exist
	// 2. drop edge and recreate edge
	// 3. insert vertexes if necessary
	// 4. perform concurrent insert and get test
	// 5. linearizability verification using procupine

	if clientNum <= 0 {
		panic("clientNum should be greater than 0")
	}

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

	checkResultSet := func(prefix string, res *nebula.ResultSet) error {
		if !res.IsSucceed() {
			return fmt.Errorf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, res.GetErrorCode(), res.GetErrorMsg())
			// log.Fatal(fmt.Sprintf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, res.GetErrorCode(), res.GetErrorMsg()))
			// panic(fmt.Sprintf("%s, ErrorCode: %v, ErrorMsg: %s", prefix, res.GetErrorCode(), res.GetErrorMsg()))
		}

		return nil
	}

	log.Info(fmt.Sprintf("creating %d clients...", clientNum))
	clients := make([]Client, clientNum)
	for i := range clients {
		c := &clients[i]
		if c.session, err = pool.GetSession(username, password); err != nil {
			panic(fmt.Sprintf("failed creating nebula session %+v", err))
		}

		c.ID = i
	}

	// create space
	log.Info(fmt.Sprintf("creating space %s...", spaceName))
	ngqlCreateSpace := fmt.Sprintf("create space if not exists %s(partition_num=1, replica_factor=3, vid_type=int)", spaceName)
	ret, err := clients[0].session.Execute(ngqlCreateSpace)
	if err != nil {
		panic(fmt.Sprintf("failed creating space %s, err: %+v", spaceName, err))
	}

	if err := checkResultSet("", ret); err != nil {
		panic(fmt.Sprintf("failed creating space %s, err: %+v", spaceName, err))
	}
	time.Sleep(1 * time.Second)

	log.Info(fmt.Sprintf("switching to space %s...", spaceName))
	// switch space for clients
	for i := range clients {
		c := &clients[i]
		rs, err := c.session.Execute(fmt.Sprintf("use %s;", spaceName))
		if err != nil {
			panic(fmt.Sprintf("error switching space: %+v", err))
		}

		if err := checkResultSet("", rs); err != nil {
			panic(fmt.Sprintf("error switching space: %+v", err))
		}
	}

	// create tag
	ngqlCreateTag := "create tag if not exists player(age int);"
	ret, err = clients[0].session.Execute(ngqlCreateTag)
	if err != nil {
		panic(fmt.Sprintf("failed createting tag: %+v", err))
	}

	if err := checkResultSet("", ret); err != nil {
		panic(fmt.Sprintf("failed createting tag: %+v", err))
	}
	time.Sleep(1 * time.Second)

	// insert vertex
	for i := 1; i <= vertexes; i++ {
		ngqlInsertVertex := fmt.Sprintf("insert vertex player(age) values %d:(%d)", i, i)
		ret, err := clients[0].session.Execute(ngqlInsertVertex)
		if err != nil {
			panic(fmt.Sprintf("failed inserting vertex: %+v", err))
		}

		if err := checkResultSet("", ret); err != nil {
			panic(fmt.Sprintf("failed inserting vertex: %+v", err))
		}
	}

	// drop edge
	log.Info(fmt.Sprintf("droping edge %s...", edgeName))
	ngqlDropEdge := fmt.Sprintf("drop edge if exists %s", edgeName)
	ret, err = clients[0].session.Execute(ngqlDropEdge)
	if err != nil {
		panic(fmt.Sprintf("failed droping edge %s, err: %+v", edgeName, err))
	}

	if err := checkResultSet("", ret); err != nil {
		panic(fmt.Sprintf("failed creating edge %s, err: %+v", edgeName, err))
	}
	time.Sleep(1 * time.Second)

	// create edge
	log.Info(fmt.Sprintf("creating edge %s...", edgeName))
	ngqlCreateEdge := fmt.Sprintf("create edge %s(idx string);", edgeName)
	ret, err = clients[0].session.Execute(ngqlCreateEdge)
	if err != nil {
		panic(fmt.Sprintf("failed creating edge %s, err: %+v", edgeName, err))
	}

	if err := checkResultSet("", ret); err != nil {
		panic(fmt.Sprintf("failed creating edge %s, err: %+v", edgeName, err))
	}
	time.Sleep(2 * time.Second)

	// 16 edges, 4 * 4
	edges := [][2]int{}
	for i := 1; i <= vertexes; i++ {
		for j := 1; j <= vertexes; j++ {
			edges = append(edges, [2]int{i, j})
		}
	}

	for _, e := range edges {
		ngqlInsertEdge := fmt.Sprintf(`insert edge %s(idx) values %d->%d:("")`, edgeName, e[0], e[1])
		fmt.Printf("%s\n", ngqlInsertEdge)
		ret, err := clients[0].session.Execute(ngqlInsertEdge)
		if err != nil {
			panic(fmt.Sprintf("failed inserting edge: %+v", err))
		}

		if err := checkResultSet("", ret); err != nil {
			panic(fmt.Sprintf("faield insert edge: %+v", err))
		}
	}

	kvPut := func(c *Client, key int, value string) error {
		src, dst := edges[key][0], edges[key][1]
		ngqlUpdateEdge := fmt.Sprintf(`update edge %d->%d of %s set idx = "%s"`, src, dst, edgeName, value)
		// fmt.Printf("%s\n", ngqlUpdateEdge)
		ret, err := c.session.Execute(ngqlUpdateEdge)
		if err != nil {
			// panic(fmt.Sprintf("failed update: %+v", err))
			return err
		}

		if err := checkResultSet("", ret); err != nil {
			// panic(fmt.Sprintf("failed update: %+v", err))
			return err
		}

		return nil
	}

	kvAppend := func(c *Client, key int, value string) error {
		src, dst := edges[key][0], edges[key][1]
		ngqlUpdateEdge := fmt.Sprintf(`update edge %d->%d of %s set idx = idx + "%s"`, src, dst, edgeName, value)
		// fmt.Printf("%s\n", ngqlUpdateEdge)
		ret, err := c.session.Execute(ngqlUpdateEdge)
		if err != nil {
			return err
		}

		if err := checkResultSet("", ret); err != nil {
			return err
		}

		return nil
	}

	kvGet := func(c *Client, key int) (string, error) {
		src, dst := edges[key][0], edges[key][1]
		ngqlGet := fmt.Sprintf("match (v1)-[e:%s]->(v2) where id(v1) == %d and id(v2) == %d return e.idx;", edgeName, src, dst)
		ret, err := c.session.Execute(ngqlGet)
		if err != nil {
			// panic(fmt.Sprintf("failed get: %+v", err))
			return "", nil
		}

		if err := checkResultSet("", ret); err != nil {
			// panic(fmt.Sprintf("failed get: %+v", err))
			return "", nil
		}

		ret.GetColNames()
		record, _ := ret.GetRowValuesByIndex(0)
		// fmt.Printf("val: %+v, colnames: %+v\n", ret, ret.GetColNames())
		// fmt.Printf("record: %+v\n", record.String())
		x, _ := record.GetValueByIndex(0)
		gg, _ := x.AsString()
		return gg, nil
	}

	// kvPut(&clients[0], 0, "hello")
	// kvAppend(&clients[0], 0, " world")
	// kvGet(&clients[0], 0)

	// now let's rocks and roll

	allOperations := []porcupine.Operation{}
	clientOperations := make([][]porcupine.Operation, len(clients))
	var wg sync.WaitGroup
	// wg.Add(clientNum)
	edgeSize := len(edges)
	wg.Add(len(clients))
	for i := range clients {
		go func(cid int, c *Client) {
			for o := 0; o < iterCount; o++ {
				typ := rand.Intn(5)
				switch typ {
				case 0:
					// put
					key := rand.Intn(edgeSize)
					value := fmt.Sprintf(" put{c: %d, o: %d}", cid, o)
					oprt := porcupine.Operation{
						ClientId: c.ID,
						Call:     time.Now().UnixNano(),
						Input: kvInput{
							OP:    1,
							Key:   fmt.Sprintf("%d", key),
							Value: value,
						},
					}

					for k := 0; k < 10; k++ {
						if err := kvPut(c, key, value); err == nil {
							oprt.Return = time.Now().UnixNano()
							oprt.Output = kvOutput{}
							clientOperations[c.ID] = append(clientOperations[c.ID], oprt)
							break
						}
					}
				case 1:
					// get
					key := rand.Intn(edgeSize)
					oprt := porcupine.Operation{
						ClientId: c.ID,
						Call:     time.Now().UnixNano(),
						Input: kvInput{
							OP:  0,
							Key: fmt.Sprintf("%d", key),
						},
					}

					for k := 0; k < 10; k++ {
						if val, err := kvGet(c, key); err == nil {
							oprt.Return = time.Now().UnixNano()
							oprt.Output = kvOutput{
								Value: val,
							}
							clientOperations[c.ID] = append(clientOperations[c.ID], oprt)
							// fmt.Printf("client %d, o %d get %d: %s\n", cid, o, key, val)
							break
						}
					}

				default:
					// append
					key := rand.Intn(edgeSize)
					value := fmt.Sprintf(" append{c: %d, o: %d}", cid, o)
					oprt := porcupine.Operation{
						ClientId: c.ID,
						Call:     time.Now().UnixNano(),
						Input: kvInput{
							OP:    2,
							Key:   fmt.Sprintf("%d", key),
							Value: value,
						},
					}

					for k := 0; k < 10; k++ {
						if err := kvAppend(c, key, value); err == nil {
							oprt.Return = time.Now().UnixNano()
							oprt.Output = kvOutput{}
							clientOperations[c.ID] = append(clientOperations[c.ID], oprt)
							break
						}
					}

				}
			}

			wg.Done()
		}(i, &clients[i])
	}

	wg.Wait()
	fmt.Printf("insert done!\n")
	for i := range clientOperations {
		allOperations = append(allOperations, clientOperations[i]...)
	}
	// d, _ := json.MarshalIndent(allOperations, "", "  ")
	// fmt.Printf("all operations: %+v\n", string(d))
	fmt.Printf("performing check...\n")
	ok := porcupine.CheckOperations(kvModel, allOperations)
	fmt.Printf("check: %t\n", ok)
	res, info := porcupine.CheckOperationsVerbose(kvModel, allOperations, 0)
	fmt.Printf("res: %+v\n", res)
	file, err := ioutil.TempFile("", "*.html")
	if err != nil {
		panic("error open file")
	}
	defer file.Close()
	fmt.Printf("path: %s\n", file.Name())
	if err := porcupine.Visualize(kvModel, info, file); err != nil {
		panic("visual failed")
	}

	for _, c := range clients {
		c.session.Release()
	}
}
