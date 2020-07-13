package bigmap

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
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

func writeI(i int, start int64) {
	if i%1000000 == 0 {
		name := os.Getenv("TEST_LOC")
		if name == "" {
			panic("NO TEST LOC PROVIDED")
		}
		write(fmt.Sprintf("%s/mem_%d", name, i))
		fmt.Println("INFO", time.Now().Unix()-start, i)
	}
}

func setEntries(b *BigMap, n int) {
	start := time.Now().Unix()
	for i := 0; i < n; i++ {
		writeI(i, start)
		s := strconv.Itoa(i)
		b.Set([]byte(s), uint64(i))
	}
}

func randomEntries(b *BigMap, n int, ratio float64) error {
	start := time.Now().Unix()
	total_till := 0
	for i := 0; i < n; i++ {
		writeI(i, start)

		prob := rand.Float64()
		if prob < ratio || total_till < 3 {
			s := strconv.Itoa(total_till)
			b.Set([]byte(s), uint64(total_till))
			total_till += 1
		} else {
			key := rand.Intn(total_till)
			s := strconv.Itoa(key)
			val := b.Get([]byte(s))

			if val.(uint64) != uint64(key) {
				return errors.New(fmt.Sprintf("Wrong Get %d", total_till))
			}
		}
	}

	return nil
}

func benchSetConfig(b *testing.B, c *Config, n int) error {
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

func benchRandomConfig(b *testing.B, c *Config, n int, ratio float64) error {
	bm, err := NewBigMap(c)
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		if err := randomEntries(bm, n, ratio); err != nil {
			return err
		}
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
	benchSetConfig(b, c, 1000)
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
	benchSetConfig(b, c, 500000000)
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
	benchSetConfig(b, c, 200000000)
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
	benchSetConfig(b, c, 500000000)
}

func BenchmarkSetBigBig1Mil(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      4,
		LenMaxMap:       1000000,
		LenPreAllocxMap: 1000000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         8,
	}
	benchSetConfig(b, c, 1000000000)
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
	benchSetConfig(b, c, 500000000)
}

func BenchmarkRand(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      8,
		LenMaxMap:       100000,
		LenPreAllocxMap: 100000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         4,
	}
	if err := benchRandomConfig(b, c, 100000000, 0.1); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func BenchmarkSet1Mil(b *testing.B) {
	c := &Config{
		NumMapShards:    32,
		NumBadgers:      8,
		LenMaxMap:       100000,
		LenPreAllocxMap: 100000,
		LenBloom:        112345678,
		LenFalsePos:     0.1,
		LenChan:         4,
	}
	benchSetConfig(b, c, 2000000000)
}
