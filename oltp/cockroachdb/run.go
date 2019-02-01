// #!/usr/bin/env gorun

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
	containerName = "cockroachdb_roach1_1"
	statsPath     = "results/cdb_%d.stat"
	outputPath    = "results/cdb_%d.out"
)

var (
	warehouses []int
)

func main() {
	parseArgs()
	run()
}

func parseArgs() {
	if len(os.Args) < 2 {
		log.Fatalln("not enough args")
	}

	for _, warehouse := range os.Args[1:] {
		arg, err := strconv.ParseInt(warehouse, 10, 32)
		if err != nil {
			log.Fatalln("cannot parse warehouse arg")
		}
		warehouses = append(warehouses, int(arg))
	}
}

func run() {
	for _, warehouse := range warehouses {
		if err := runQuery(warehouse); err != nil {
			log.Printf("error while running for %d warehouses: %v", warehouse, err)
		}
		log.Printf("%d warehouses completed.", warehouse)
	}
}

func runQuery(warehouse int) error {
	ctx, monitoringCancelFunc := context.WithCancel(context.Background())
	defer monitoringCancelFunc()
	go monitorSystem(ctx, warehouse)

	return runWorkload(warehouse)
}

func runWorkload(warehouse int) error {
	args := []string{
		"exec",
		"-t",
		containerName,
		"bash",
		"-c",
		fmt.Sprintf("./cockroach workload init tpcc --drop --warehouses %d", warehouse),
	}
	cmd := exec.Command("docker", args...)
	cmd.CombinedOutput()
	log.Printf("%d warehouses loaded completely", warehouse)

	args = []string{
		"exec",
		"-t",
		containerName,
		"bash",
		"-c",
		fmt.Sprintf("./cockroach workload run tpcc --warehouses %d --duration 150s", warehouse),
	}
	cmd = exec.Command("docker", args...)
	output, err := cmd.CombinedOutput()

	outputFilePath := fmt.Sprintf(outputPath, warehouse)
	ioutil.WriteFile(outputFilePath, output, 0644)
	return err
}

func monitorSystem(ctx context.Context, warehouse int) error {
	outputFilePath := fmt.Sprintf(statsPath, warehouse)
	args := []string{
		"-lcmdrsyTt",
		"--full",
		"--output",
		outputFilePath,
	}
	return exec.CommandContext(ctx, "dstat", args...).Run()
}
