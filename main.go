package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/file/s3file"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Panic(err)
	}
	var (
		in    = bufio.NewScanner(f)
		paths []string
		wg    sync.WaitGroup
	)
	for in.Scan() {
		paths = append(paths, strings.TrimSpace(in.Text()))
	}
	file.RegisterImplementation("s3", func() file.Implementation {
		return s3file.NewImplementation(s3file.NewDefaultProvider(session.Options{}), s3file.Options{})
	})

	type state struct {
		path         string // file to read.
		nByte        int64  // cumulative # of bytes read
		lastUpdateNS int64  // last status update. Nanoseconds since 1970/1/1
	}
	states := make([]state, len(paths))
	for i, path := range paths {
		states[i].path = path
	}
	// Start reading files in parallel.
	for i := range states {
		wg.Add(1)
		go func(s *state) {
			ctx := context.Background()
			log.Printf("%s: start", s.path)
			defer wg.Done()
			f, err := file.Open(ctx, s.path)
			if err != nil {
				log.Panic(err)
			}
			var (
				buf = make([]byte, 4<<20)
				r   = f.Reader(context.Background())
			)
			for {
				n, err := r.Read(buf)
				now := time.Now()
				s.nByte += int64(n)
				s.lastUpdateNS = now.UnixNano()
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Panicf("%s: %v", s.path, err)
				}
			}
			log.Printf("%s: done, %d bytes", s.path, s.nByte)
		}(&states[i])
	}
	// Report stats every second.
	go func() {
		save := func(dst []state) {
			for i := range states {
				dst[i].path = states[i].path
				dst[i].nByte = atomic.LoadInt64(&states[i].nByte)
				dst[i].lastUpdateNS = atomic.LoadInt64(&states[i].lastUpdateNS)
			}
		}
		last := make([]state, len(paths))
		lastNS := time.Now().UnixNano()
		save(last)
		curr := make([]state, len(paths))
		for {
			time.Sleep(1 * time.Second)
			nowNS := time.Now().UnixNano()
			save(curr)
			var totalBytes int64
			for i := range curr {
				if curr[i].lastUpdateNS != 0 && nowNS-curr[i].lastUpdateNS > 20*1000000000 {
					log.Printf("%s: stuck for %fs", curr[i].path, float64(nowNS-curr[i].lastUpdateNS)/1000000000.0)
				}
				totalBytes += (curr[i].nByte - last[i].nByte)
			}
			log.Printf("throughput %fMiB/s",
				(float64(totalBytes)/1024.0/1024.0)/(float64(nowNS-lastNS)/1000000000.0))
			curr, last = last, curr
		}
	}()
	wg.Wait()
}
