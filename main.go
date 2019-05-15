package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"strings"
	"sync"

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
	for _, path := range paths {
		wg.Add(1)
		go func(path string) {
			ctx := context.Background()
			log.Printf("%s: start", path)
			defer wg.Done()
			f, err := file.Open(ctx, path)
			if err != nil {
				log.Panic(err)
			}
			var (
				buf        = make([]byte, 1<<20)
				r          = f.Reader(context.Background())
				totalBytes = 0
			)
			for {
				n, err := r.Read(buf)
				totalBytes += n
				if err != nil {
					if err == io.EOF {
						break
					}
					log.Panicf("%s: %v", path, err)
				}
			}
			log.Printf("%s: done, %d bytes", path, totalBytes)
		}(path)
	}
	wg.Wait()
}
