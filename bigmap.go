package bigmap

import (
	"encoding/binary"
	"errors"
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
	writers *writer

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
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
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
		writers: nil,
		cache:   cache,
		maxCap:  config.LenMaxMap,
		pool: sync.Pool{New: func() interface{} {
			return make(map[string]interface{}, config.LenPreAllocxMap)
		}},
		dir: tmpDir,
	}

	dir, err := ioutil.TempDir(tmpDir, "shard")
	if err != nil {
		return nil, err
	}

	opt := badger.LSMOnlyOptions(dir).WithSyncWrites(false)

	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	bm.writers = &writer{
		dir:    dir,
		db:     db,
		inCh:   make(chan *Input, config.LenChan),
		inBuff: newChanBuffer(3 * config.LenChan),
		bl:     z.NewBloomFilter(config.LenBloom, config.LenFalsePos),
	}

	if config.LenBloom == 0 {
		bm.writers.bl = nil
	}

	for i := range bm.shards {
		bm.shards[i] = &shard{
			dict: bm.pool.Get().(map[string]interface{}),
		}
	}

	bm.varToBytes = func(value interface{}) []byte {
		var uidBuf [8]byte
		binary.BigEndian.PutUint64(uidBuf[:], value.(uint64))
		return uidBuf[:]
	}

	bm.cacheCost = func(key string) int64 {
		// reducing this causes OOM but increases speed
		return 10 * int64(len(key))
	}

	dump := func(h *shard, k int) {
		writer := bm.writers.db.NewWriteBatch()
		uidMap := (bm.writers.inBuff.buffer[k])
		for key, value := range uidMap {
			bm.cache.Set(key, value, bm.cacheCost(key))
			if err := writer.Set([]byte(key), bm.varToBytes(value)); err != nil {
				panic(err)
			}
		}

		// need to put a lock because if we remove the data from memory before it's 
		// flushed to badger, reads will get wrong value
		bm.writers.Lock()
		bm.pool.Put(uidMap)
		bm.writers.inBuff.remove(k)
		writer.Flush()
		bm.writers.Unlock()
	}

	bm.inwg.Add(1)
	go func(bm *BigMap) {
		for i := range bm.writers.inCh {
			dump(i.sh, i.bP)
		}
		bm.inwg.Done()
	}(bm)

	return bm, nil
}

func (b *BigMap) getShard(key []byte) *shard {
	fp := farm.Fingerprint32([]byte(key))
	shard_i := fp % uint32(len(b.shards))
	return b.shards[shard_i]
}

func (b *BigMap) Get(key []byte) interface{} {
	sh := b.getShard(key)

	sh.RLock()
	val := sh.dict[string(key)]
	sh.RUnlock()

	if val != nil {
		return val
	}

	fp := farm.Fingerprint64(key)

	var valCopy []byte
	if b.writers.bl.Has(fp) {
		b.writers.RLock()
		for _, i := range b.writers.inBuff.buffer {
			if i == nil {
				continue
			}
			val := i[string(key)]
			if val != nil {
				b.writers.RUnlock()
				return val
			}
		}
		b.writers.RUnlock()

		if b.cache != nil {
			if valI, ok := b.cache.Get(key); ok && valI != nil {
				return valI
			}
		}

		err := b.writers.db.View(func(txn *badger.Txn) error {
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
	sh := b.getShard(key)
	sh.Lock()
	defer sh.Unlock()

	sh.dict[string(key)] = value

	if len(sh.dict) > b.maxCap {
		b.writers.Lock()
		bP := b.writers.inBuff.insert(sh.dict)
		b.writers.Unlock()
		inp := &Input{sh: sh, bP: bP}
		if b.writers.bl != nil {
			for key := range sh.dict {
				b.writers.bl.Add(farm.Fingerprint64([]byte(key)))
			}
		}
		b.writers.inCh <- inp
		// benchmarked to be faster than re-initializing a new map and letting this get gc
		// since no gc, uses less memory
		sh.dict = nil
		sh.dict = b.pool.Get().(map[string]interface{})
		for key := range sh.dict {
			delete(sh.dict, key)
		}
	}
}

func (b *BigMap) Finish() {
	close (b.writers.inCh)
	b.inwg.Wait()
	b.writers.db.Close()
	os.RemoveAll(b.writers.dir)
	os.RemoveAll(b.dir)
}
