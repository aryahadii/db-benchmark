#!/usr/bin/env gorun

package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
)

const (
	containerName = "postgres_postgres_1"
	databaseName  = "tpcc"
	statsPath     = "results/pg_%d_%d.stat"
	outputPath    = "results/pg_%d_%d.out"
)

var (
	rates      []int
	warehouses int
)

func main() {
	parseArgs()
	run()
}

func parseArgs() {
	if len(os.Args) < 3 {
		log.Fatalln("not enough args")
	}

	arg1, err := strconv.ParseInt(os.Args[1], 10, 32)
	if err != nil {
		log.Fatalln("cannot parse warehouses arg")
	}
	warehouses = int(arg1)

	for _, rate := range os.Args[2:] {
		arg, err := strconv.ParseInt(rate, 10, 32)
		if err != nil {
			log.Fatalln("cannot parse rate arg")
		}
		rates = append(rates, int(arg))
	}
}

func run() {
	for _, rate := range rates {
		if err := runQuery(warehouses, rate); err != nil {
			log.Printf("error while running for (%d, %d): %v", warehouses, rate, err)
		}
	}
}

func runQuery(warehouses, rate int) error {
	ctx, monitoringCancelFunc := context.WithCancel(context.Background())
	defer monitoringCancelFunc()
	go monitorSystem(ctx, warehouses, rate)

	return runOLTPBench(warehouses, rate)
}

func runOLTPBench(warehouses, rate int) error {
	confPath, err := createOLTPBenchConfig(warehouses, rate)
	if err != nil {
		return err
	}

	args := []string{
		"-b",
		"tpcc",
		"-c",
		confPath,
		"--clear",
		"true",
		"--create",
		"true",
	}
	cmd := exec.Command("./oltpbenchmark", args...)
	_, err = cmd.CombinedOutput()

	args = []string{
		"-b",
		"tpcc",
		"-c",
		confPath,
		"--load",
		"true",
	}
	cmd = exec.Command("./oltpbenchmark", args...)
	_, err = cmd.CombinedOutput()

	outputFilePath := fmt.Sprintf(outputPath, warehouses, rate)
	args = []string{
		"-b",
		"tpcc",
		"-c",
		confPath,
		"--execute",
		"true",
		"-s",
		"5",
		"-o",
		outputFilePath,
	}
	cmd = exec.Command("./oltpbenchmark", args...)
	_, err = cmd.CombinedOutput()
	return err
}

func createOLTPBenchConfig(warehouses, rate int) (string, error) {
	path := "tpcc.conf"
	conf := fmt.Sprintf(config, warehouses*10, warehouses, rate)
	return path, ioutil.WriteFile(path, []byte(conf), os.ModePerm)
}

func monitorSystem(ctx context.Context, warehouses, rate int) error {
	outputFilePath := fmt.Sprintf(statsPath, warehouses, rate)
	args := []string{
		"-lcmdrsyTt",
		"--full",
		"--output",
		outputFilePath,
	}
	return exec.CommandContext(ctx, "dstat", args...).Run()
}

const (
	config = `<?xml version="1.0"?>
<parameters>
    <dbtype>postgres</dbtype>
    <driver>org.postgresql.Driver</driver>
    <DBUrl>jdbc:postgresql://localhost:5432/tpcc</DBUrl>
    <DBName>tpcc</DBName>
    <username>root</username>
    <password>root</password>
    <terminals>%d</terminals>

    <scalefactor>%d</scalefactor>


    <transactiontypes>
    	<transactiontype>
    		<name>NewOrder</name>
    	</transactiontype>
    	<transactiontype>
    		<name>Payment</name>
    	</transactiontype>
    	<transactiontype>
    		<name>OrderStatus</name>
    	</transactiontype>
    	<transactiontype>
    		<name>Delivery</name>
    	</transactiontype>
    	<transactiontype>
    		<name>StockLevel</name>
    	</transactiontype>
    </transactiontypes>

    <works>
        <work>
          <time>180</time>
          <rate>%d</rate>
	  <weights>45,43,4,4,4</weights>
        </work>
    </works>

</parameters>
	`
)
