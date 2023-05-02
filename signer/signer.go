package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const MaxTh = 6
func ExecutePipeline(jobs ...job) {
	chans := make([]chan interface{}, len(jobs)+1)
	for i := range chans {
		chans[i] = make(chan interface{}, 10)
	}
	var wg sync.WaitGroup
	wg.Add(len(jobs))
	for i, v := range jobs {
		go worker(chans[i], chans[i+1], v, &wg)
	}
	wg.Wait()
}

func worker(in, out chan interface{}, j job, wg *sync.WaitGroup) {
	defer func(){
		close(out)
		wg.Done()
	}()
	j(in, out)
}

func crc32Worker(data string, destination *string, wg *sync.WaitGroup) {
	*destination = DataSignerCrc32(data)
	wg.Done()
}

func getMd5(data string, md5Mutex *sync.Mutex) string {
	md5Mutex.Lock()
	res := DataSignerMd5(data)
	md5Mutex.Unlock()
	return res
}

func getCrc32Concat(data, datamd5 string) string {
	var crc32, crc32md5 string
	var wg sync.WaitGroup
	wg.Add(2)
	go crc32Worker(data, &crc32, &wg)
	go crc32Worker(datamd5, &crc32md5, &wg)
	wg.Wait()
	return crc32+"~"+crc32md5
}

func getSingleHash(i interface{}, out chan interface{}, shwg *sync.WaitGroup, md5Mutex *sync.Mutex) {
	defer shwg.Done()
	data := strconv.Itoa(i.(int))
	datamd5 := getMd5(data, md5Mutex)
	out <- getCrc32Concat(data, datamd5)
}

func SingleHash(in, out chan interface{}) {
	var (
		shwg sync.WaitGroup
		md5Mutex sync.Mutex
	)
	defer shwg.Wait()
	for i := range in {
		shwg.Add(1)
		go getSingleHash(i, out, &shwg, &md5Mutex)
	}
}

func getMultiHash(i interface{}, out chan interface{}, mhwg *sync.WaitGroup) {
	defer mhwg.Done()
	input := make([]string, MaxTh)
	var wg sync.WaitGroup
	wg.Add(MaxTh)
	for j := 0; j < MaxTh; j++ {
		go crc32Worker(strconv.Itoa(j)+i.(string), &input[j], &wg)
	}
	wg.Wait()
	res := strings.Join(input, "")
	out <- res
}

func MultiHash(in, out chan interface{}) {
	var mhwg sync.WaitGroup
	defer mhwg.Wait()
	for i := range in {
		mhwg.Add(1)
		go getMultiHash(i, out, &mhwg)
	}
}

func CombineResults(in, out chan interface{}) {
	res := make([]string, 0)
	for i := range in {
		res = append(res, i.(string))
	}
	sort.Strings(res)
	out <- strings.Join(res, "_")
}