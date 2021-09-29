package main

import (
	"log"

	"github.com/tecbot/gorocksdb"
)

/*
 * /Users/wenlinwu/tmp/data.bak
 */
func CheckRocksdb(spaceName string, edgeName string, rocksDataDirs []string) {
	rocksDataDirs = []string{
		"/Users/wenlinwu/tmp/data.bak/storaged.0/nebula/1/data",
		"/Users/wenlinwu/tmp/data.bak/storaged.1/nebula/1/data",
		"/Users/wenlinwu/tmp/data.bak/storaged.2/nebula/1/data",
	}
}

func loadEdges(rocksdbDataDir string) {

}

func main() {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	// opts.SetCompression(gorocksdb.NoCompression)
	opts.SetWriteBufferSize(671088640)
	dataDir := "/Users/wenlinwu/tmp/data.bak/storaged.0/nebula/1/data"
	db, err := gorocksdb.OpenDb(opts, dataDir)
	// wopt := gorocksdb.NewDefaultWriteOptions()
	if err != nil {
		log.Printf("%v\n", err)
	}
	defer db.Close()
	// db.Put(wopt, []byte("data"), []byte("value"))
}
