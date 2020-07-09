package bigmap

import (
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

func write(name string) {
	f, err := os.Create(name)
	if err != nil {
		log.Fatal("could not create memory profile: ", err)
	}
	defer f.Close() // error handling omitted for example

	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal("could not write memory profile: ", err)
	}
}

func TestMain(m *testing.M) {
	fmt.Println("Here")
	code := m.Run()
	os.Exit(code)
}

func setEntries(b *BigMap, n int) {
	start := time.Now().Unix()
	for i := 0; i < n; i++ {
		if i%1000000 == 0 {
			name := os.Getenv("TEST_LOC")
			if name == "" {
				panic("NO TEST LOC PROVIDED")
			}
			write(fmt.Sprintf("%s/mem_%d", name, i))
			fmt.Println("INFO", time.Now().Unix()-start, i)
		}
		s := strconv.Itoa(i)
		b.Set([]byte(s), uint64(i))
	}
}

func benchConfig(b *testing.B, c *Config, n int) error {
	bm, err := NewBigMap(c)
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		setEntries(bm, n)
	}

	bm.Finish()

	return nil
}

func BenchmarkSetSmol(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      8,
		LenMaxMap:       1000000,
		LenPreAllocxMap: 1000000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         16,
	}
	benchConfig(b, c, 1000)
}

func BenchmarkSetBigNoRes(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      0,
		LenMaxMap:       10000000000,
		LenPreAllocxMap: 0,
		LenBloom:        0,
		LenFalsePos:     0.1,
		LenChan:         16,
	}
	benchConfig(b, c, 500000000)
}

func BenchmarkSetNoRes(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      0,
		LenMaxMap:       10000000000,
		LenPreAllocxMap: 0,
		LenBloom:        0,
		LenFalsePos:     0.1,
		LenChan:         16,
	}
	benchConfig(b, c, 200000000)
}

func BenchmarkSetNoAllocBig1Mil(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      8,
		LenMaxMap:       1000000,
		LenPreAllocxMap: 1000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         4,
	}
	benchConfig(b, c, 500000000)
}

func BenchmarkSetBigBig1Mil(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      8,
		LenMaxMap:       1000000,
		LenPreAllocxMap: 1000000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         4,
	}
	benchConfig(b, c, 1000000000)
}

func BenchmarkSetBig1Mil(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      8,
		LenMaxMap:       1000000,
		LenPreAllocxMap: 1000000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         4,
	}
	benchConfig(b, c, 500000000)
}

func BenchmarkSet1Mil(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      8,
		LenMaxMap:       1000000,
		LenPreAllocxMap: 1000000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         4,
	}
	benchConfig(b, c, 200000000)
}
