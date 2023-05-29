package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"
)

type result struct {
	query string
	index int
	res   http.Response
	err   error
}

func FetchContentParallel(urls []string, concurrencyLimit int) []result {
	semaphoreChan := make(chan struct{}, concurrencyLimit)
	resultsChan := make(chan *result)

	defer func() {
		close(semaphoreChan)
		close(resultsChan)
	}()

	for i, url := range urls {
		go func(i int, url string) {
			semaphoreChan <- struct{}{}

			res, err := http.Get(url)
			queryString := strings.Split(url, "=")
			r := &result{queryString[1], i, *res, err}

			resultsChan <- r
			<-semaphoreChan
		}(i, url)
	}

	var results []result
	for {
		r := <-resultsChan
		results = append(results, *r)

		if len(results) == len(urls) {
			break
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].index < results[j].index
	})
	return results
}

type ASN struct {
	ASPrefixedNumber string
}

type ASNRouteInformation struct {
	ASN  string
	Data string
}

var RequestURITemplate = "https://stat.ripe.net/data/announced-prefixes/data.json?resource="
var ASNList = "asn.txt"
var OutputDir = "results"

func readAsnInformation(filepath string) []ASN {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	results := make([]ASN, 0)

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		content := strings.Split(scanner.Text(), " ")
		results = append(results, ASN{ASPrefixedNumber: fmt.Sprintf("AS%s", content[0])})
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return results
}

func PrepareURLs(queries []ASN) []string {
	res := make([]string, 0)

	for _, asnQuery := range queries {
		if asnQuery.ASPrefixedNumber != "" {
			q := fmt.Sprintf("%s%s", RequestURITemplate, asnQuery.ASPrefixedNumber)
			res = append(res, q)
		}
	}

	return res
}

func ensureDir(dirName string) error {
	err := os.Mkdir(dirName, os.ModePerm)
	if err == nil {
		return nil
	}
	if os.IsExist(err) {
		info, err := os.Stat(dirName)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("path exists but is not a directory")
		}
		return nil
	}
	return err
}

func WriteResponse(results []result) {
	responses := make([]ASNRouteInformation, 0)
	err := ensureDir(OutputDir)
	if err != nil {
		panic("Failed")
	}
	start := time.Now()
	for _, r := range results {
		res := r.res
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("%v\n", fmt.Errorf("failed to read body: %v\n", r.index))
			continue
		}
		a := ASNRouteInformation{
			ASN:  r.query,
			Data: string(body),
		}
		responses = append(responses, a)
	}
	end := time.Now()

	outputFile := fmt.Sprintf("%s/test-%v.log", OutputDir, time.Now().Unix())

	f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Printf("%v\n", fmt.Errorf("failed to create the output file %v\n", err))
		os.Exit(-1)
	}

	datawriter := bufio.NewWriter(f)

	defer f.Close()

	for _, line := range responses {
		lineString, err := json.Marshal(line)
		_, err = datawriter.WriteString(string(lineString) + "\n")
		if err != nil {
			log.Fatal(err)
		}
	}

	err = datawriter.Flush()
	if err != nil {
		fmt.Printf("%v\n", fmt.Errorf("failed to flush contents to disk"))
	}

	fmt.Printf("Time to process responses and write to disk: %v\n", end.Sub(start))
}

func main() {
	asns := readAsnInformation(ASNList)
	fmt.Printf("Number of ASNs: %d\n", len(asns))
	urls := PrepareURLs(asns)
	concurrency := 2 * runtime.NumCPU()
	startTime := time.Now()
	results := FetchContentParallel(urls, concurrency)
	endTime := time.Now()
	fmt.Printf("Time taken : %v\n", endTime.Sub(startTime))
	fmt.Printf("Number of results : %v\n", len(results))
	WriteResponse(results)
}
