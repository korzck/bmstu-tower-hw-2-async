package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

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
	defer close(out)
	j(in, out)
	wg.Done()
}

func crc32Worker(data string, destination *string, wg *sync.WaitGroup) {
	*destination = DataSignerCrc32(data)
	wg.Done()
}

func SingleHash(in, out chan interface{}) {
	m := &sync.Mutex{}
	var shwg sync.WaitGroup
	for i := range in {
		shwg.Add(1)
		go func(i interface{}, out chan interface{}, m *sync.Mutex) {
			data := strconv.Itoa(i.(int))
			m.Lock()
			datamd5 := DataSignerMd5(data)
			m.Unlock()
			var crc32, crc32md5 string
			var wg sync.WaitGroup
			wg.Add(2)
			go crc32Worker(data, &crc32, &wg)
			go crc32Worker(datamd5, &crc32md5, &wg)
			wg.Wait()
			res := crc32+"~"+crc32md5
			out <- res
			shwg.Done()
		}(i, out, m)
	}
	shwg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var mhwg sync.WaitGroup
	for i := range in {
		mhwg.Add(1)
		go func(i interface{}, out chan interface{}) {
			input := make([]string, 6)
			var wg sync.WaitGroup
			wg.Add(6)
			for j := 0; j < 6; j++ {
				go crc32Worker(strconv.Itoa(j)+i.(string), &input[j], &wg)
			}
			wg.Wait()
			res := strings.Join(input, "")
			out <- res
			mhwg.Done()
		}(i, out)
	}
	mhwg.Wait()
}

func CombineResults(in, out chan interface{}) {
	res := make([]string, 0)
	for i := range in {
		res = append(res, i.(string))
	}
	sort.Strings(res)
	out <- strings.Join(res, "_")
}