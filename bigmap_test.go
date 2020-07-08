package bigmap

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func setEntries(b *BigMap, n int) {
	start := time.Now().Unix()
	for i := 0; i < n; i++ {
		if i%1000000 == 0 {
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

//func BenchmarkSetSmol(b *testing.B) {
//	c := &Config{
//		NumMapShards:    32,
//		NumBadgers:      8,
//		LenMaxMap:       1000000,
//		LenPreAllocxMap: 1000000,
//		LenBloom:        112345678,
//		LenFalsePos:     0.1,
//		LenChan:         16,
//	}
//	benchConfig(b, c, 1000)
//}

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
