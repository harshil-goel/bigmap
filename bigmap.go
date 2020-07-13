package bigmap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sync"

	"net/http"
	_ "net/http/pprof"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/dgraph-io/ristretto/z"
	"github.com/dgryski/go-farm"
)

type shard struct {
	sync.RWMutex

	dict map[string]interface{}
}

type writer struct {
	sync.RWMutex

	dir string
	db  *badger.DB
	bl  *z.Bloom

	inCh   chan *Input
	inBuff *channelBuffer
}

type channelBuffer struct {
	buffer []map[string]interface{}
}

func newChanBuffer(n int) *channelBuffer {
	cb := &channelBuffer{buffer: make([]map[string]interface{}, n, n)}
	for i := 0; i < n; i++ {
		cb.buffer[i] = nil
	}
	return cb
}

func (b *channelBuffer) insert(inp map[string]interface{}) int {
	for i := 0; i < len(b.buffer); i++ {
		if b.buffer[i] != nil {
			continue
		}
		b.buffer[i] = inp
		return i
	}

	panic(errors.New("Maximum limit reached"))
}

func (b *channelBuffer) remove(i int) {
	b.buffer[i] = nil
}

type BigMap struct {
	shards  []*shard
	writers []*writer

	pool  sync.Pool
	cache *ristretto.Cache
	inwg  sync.WaitGroup

	varToBytes func(interface{}) []byte
	cacheCost  func(string) int64

	maxCap int
	dir    string
}

type Config struct {
	NumMapShards int
	NumBadgers   int

	LenMaxMap       int
	LenPreAllocxMap int
	LenBloom        float64
	LenFalsePos     float64
	LenChan         int
}

type Input struct {
	sh *shard
	bP int
}

func NewBigMap(config *Config) (*BigMap, error) {
	runtime.SetBlockProfileRate(1)

	//TODO reuse old badger
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e8,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 28, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	tmpDir, err := ioutil.TempDir(".", "tmp")
	if err != nil {
		return nil, err
	}

	bm := &BigMap{
		shards:  make([]*shard, config.NumMapShards),
		writers: make([]*writer, config.NumBadgers),
		cache:   cache,
		maxCap:  config.LenMaxMap,
		pool: sync.Pool{New: func() interface{} {
			return make(map[string]interface{}, config.LenPreAllocxMap)
		}},
		dir: tmpDir,
	}

	for i := range bm.writers {
		dir, err := ioutil.TempDir(tmpDir, fmt.Sprintf("shard-%d", i))
		if err != nil {
			return nil, err
		}

		opt := badger.LSMOnlyOptions(dir).WithSyncWrites(false)

		db, err := badger.Open(opt)
		if err != nil {
			return nil, err
		}

		bm.writers[i] = &writer{
			dir:    dir,
			db:     db,
			inCh:   make(chan *Input, config.LenChan),
			inBuff: newChanBuffer(3 * config.LenChan),
			bl:     z.NewBloomFilter(config.LenBloom, config.LenFalsePos),
		}

		if config.LenBloom == 0 {
			bm.writers[i].bl = nil
		}
	}

	for i := range bm.shards {
		bm.shards[i] = &shard{
			dict: bm.pool.Get().(map[string]interface{}),
		}
	}

	if len(bm.writers) == 0 {
		bm.writers = []*writer{nil}
	}

	bm.varToBytes = func(value interface{}) []byte {
		var uidBuf [8]byte
		binary.BigEndian.PutUint64(uidBuf[:], value.(uint64))
		return uidBuf[:]
	}

	bm.cacheCost = func(key string) int64 {
		return 2 * int64(len(key))
	}

	dump := func(h *shard, k, i int) {
		writer := bm.writers[i].db.NewWriteBatch()
		uidMap := (bm.writers[i].inBuff.buffer[k])
		for key, value := range uidMap {
			bm.cache.Set(key, value, bm.cacheCost(key))
			if err := writer.Set([]byte(key), bm.varToBytes(value)); err != nil {
				panic(err)
			}
		}

		bm.writers[i].Lock()
		bm.pool.Put(uidMap)
		bm.writers[i].inBuff.remove(k)
		writer.Flush()
		bm.writers[i].Unlock()
	}

	bm.inwg.Add(config.NumBadgers)
	for i := 0; i < int(config.NumBadgers); i++ {
		go func(bm *BigMap, bindex int) {
			for i := range bm.writers[bindex].inCh {
				dump(i.sh, i.bP, bindex)
			}
			bm.inwg.Done()
		}(bm, i)
	}

	return bm, nil
}

func (b *BigMap) getShardAndWriterI(key []byte) (uint32, uint32) {
	fp := farm.Fingerprint32([]byte(key))
	shard_i := fp % uint32(len(b.shards))
	writer_i := fp % uint32(len(b.writers))
	return shard_i, writer_i
}

func (b *BigMap) getShardAndWriter(key []byte) (*shard, *writer) {
	fp := farm.Fingerprint32([]byte(key))
	shard_i := fp % uint32(len(b.shards))
	writer_i := fp % uint32(len(b.writers))
	return b.shards[shard_i], b.writers[writer_i]
}

func (b *BigMap) Get(key []byte) interface{} {
	sh, bsh := b.getShardAndWriter(key)

	sh.RLock()
	val := sh.dict[string(key)]
	sh.RUnlock()

	if val != nil {
		return val
	}

	fp := farm.Fingerprint64(key)

	var valCopy []byte
	if bsh.bl != nil && bsh.bl.Has(fp) {
		bsh.RLock()
		for _, i := range bsh.inBuff.buffer {
			if i == nil {
				continue
			}
			val := i[string(key)]
			if val != nil {
				bsh.RUnlock()
				return val
			}
		}
		bsh.RUnlock()

		if b.cache != nil {
			if valI, ok := b.cache.Get(key); ok && valI != nil {
				return valI
			}
		}

		err := bsh.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(string(key)))
			if err != nil {
				return err
			}

			return item.Value(func(val []byte) error {
				valCopy = append([]byte{}, val...)
				return nil
			})
		})

		if err == nil {
			val = binary.BigEndian.Uint64(valCopy)
			b.cache.Set(key, val, 10*int64(len(key)))
			return val
		}
	}

	return nil
}

func (b *BigMap) Set(key []byte, value interface{}) {
	sh, bsh := b.getShardAndWriter(key)
	sh.Lock()
	defer sh.Unlock()

	sh.dict[string(key)] = value

	if len(sh.dict) > b.maxCap && bsh != nil {
		bsh.Lock()
		bP := bsh.inBuff.insert(sh.dict)
		bsh.Unlock()
		inp := &Input{sh: sh, bP: bP}
		if bsh.bl != nil {
			for key := range sh.dict {
				bsh.bl.Add(farm.Fingerprint64([]byte(key)))
			}
		}
		bsh.inCh <- inp
		sh.dict = nil
		sh.dict = b.pool.Get().(map[string]interface{})
		for key := range sh.dict {
			delete(sh.dict, key)
		}
	}
}

func (b *BigMap) Finish() {
	for _, i := range b.writers {
		if i == nil {
			continue
		}
		close(i.inCh)
	}
	b.inwg.Wait()
	for _, i := range b.writers {
		if i == nil {
			continue
		}
		i.db.Close()
		os.RemoveAll(i.dir)
	}
	os.RemoveAll(b.dir)
}
