//
// go build main.go
// ./main -t foo -p 16
//
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

var (
	data        []byte
	producers   = flag.Int("p", 1, "number of producers")
	brokers     = flag.String("b", "127.0.0.1:9092", "brokers")
	topic       = flag.String("t", "", "topic")
	randomBytes = flag.Int("r", 10<<20, "size of random bytes buffer")
	records     = flag.Int("n", 0, "number of producer records (<= 0: unlimited)")
	keySize     = flag.Int("k", 36, "key size")
	valueSize   = flag.Int("v", 300, "value size")

	rateRecords int64
	rateBytes   int64
)

func randBytes(n int) []byte {
	ascii := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = ascii[rand.Intn(len(ascii))]
	}
	return b
}

func randView(n int, data []byte) []byte {
	s := rand.Intn(len(data) - n)
	return data[s : s+n]
}

func init() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	data = randBytes(*randomBytes)
}

func producer(wg *sync.WaitGroup) {
	defer wg.Done()

	cmd := exec.Command("kafkacat",
		"-P",
		"-b", *brokers,
		"-X", "batch.size=1048576",
		"-X", "batch.num.messages=1000",
		"-X", "linger.ms=1000",
		"-X", "socket.timeout.ms=299999",
		"-t", *topic,
		"-z", "zstd",
		"-K:")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for i := 0; i < *records; i++ {
			stdin.Write(randView(*keySize, data))
			io.WriteString(stdin, ":")
			stdin.Write(randView(*valueSize, data))
			io.WriteString(stdin, "\n")
			atomic.AddInt64(&rateRecords, 1)
			atomic.AddInt64(&rateBytes, int64(*keySize+*valueSize))
		}
		stdin.Close()
	}()

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s\n", out)
}

func MovingExpAvg(value, oldValue, fdtime, ftime float64) float64 {
	alpha := 1.0 - math.Exp(-fdtime/ftime)
	r := alpha*value + (1.0-alpha)*oldValue
	return r
}

func printRate() {
	for range time.Tick(time.Second * 2) {
		recs := atomic.SwapInt64(&rateRecords, 0)
		bytes := atomic.SwapInt64(&rateBytes, 0)
		fmt.Printf("%0.2f MiB/s; %0.2fk records/s\n", float64(bytes)/(1024*1024), float64(recs)/1000)
	}
}

func main() {
	if *topic == "" {
		fmt.Println("topic not specified")
		return
	}

	if *records <= 0 {
		*records = math.MaxInt32
	}

	var wg sync.WaitGroup

	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go producer(&wg)
	}

	go printRate()

	wg.Wait()
}
