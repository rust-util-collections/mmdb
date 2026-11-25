package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func readAll(db *pebble.DB, count int) {
	var key [8]byte
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint64(key[:], uint64((i*8_191)%count))
		value, closer, err := db.Get(key[:])
		must(err)
		if len(value) != 128 || value[0] != 0x42 {
			panic("value mismatch")
		}
		must(closer.Close())
	}
}

func main() {
	if len(os.Args) != 3 && (len(os.Args) != 4 || os.Args[3] != "sync") {
		panic("expected DIR WRITERS [sync]")
	}
	count := 131_072
	writeOptions := pebble.NoSync
	if len(os.Args) == 4 {
		count = 8_192
		writeOptions = pebble.Sync
	}
	workers, err := strconv.Atoi(os.Args[2])
	must(err)
	if workers != 1 && workers != 8 {
		panic("expected 1 or 8 workers")
	}

	cache := pebble.NewCache(64 << 20)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:         cache,
		MemTableSize:  64 << 20,
		ErrorIfExists: true,
	}
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = 4096
		opts.Levels[i].Compression = func() *sstable.CompressionProfile { return sstable.NoCompression }
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(12)
	}
	db, err := pebble.Open(os.Args[1], opts)
	must(err)

	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(workers)
	done.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer done.Done()
			var key [8]byte
			value := bytes.Repeat([]byte{0x42}, 128)
			ready.Done()
			<-start
			for i := 0; i < count/workers; i++ {
				binary.BigEndian.PutUint64(key[:], uint64(((i*workers+worker)*65_537)%count))
				must(db.Set(key[:], value, writeOptions))
			}
		}(worker)
	}
	ready.Wait()
	writeStart := time.Now()
	close(start)
	done.Wait()
	writeSeconds := time.Since(writeStart).Seconds()
	must(db.Flush())
	metrics := db.Metrics()
	counts := make([]int64, len(metrics.Levels))
	for i, level := range metrics.Levels {
		counts[i] = level.TablesCount
	}

	readAll(db, count)
	readStart := time.Now()
	readAll(db, count)
	fmt.Printf("put_seconds=%.9f get_seconds=%.9f sst_by_level=%v\n",
		writeSeconds, time.Since(readStart).Seconds(), counts)
	must(db.Close())
}
