// #!/usr/bin/env gorun

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
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
	if len(os.Args) < 3 {
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
	_, err := cmd.CombinedOutput()
	log.Printf("%d warehouses loaded completely", warehouse)

	args = []string{
		"exec",
		"-t",
		containerName,
		"bash",
		"-c",
		fmt.Sprintf("./cockroach workload run tpcc --warehouses %d", warehouse),
	}
	cmd = exec.Command("docker", args...)
	output, err := cmd.StdoutPipe()

	// Timeout
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case <-time.After(5 * time.Minute):
		if err := cmd.Process.Kill(); err != nil {
			return fmt.Errorf("failed to kill process: %v", err)
		}
	case err := <-done:
		if err != nil {
			return fmt.Errorf("process finished with error: %v", err)
		}
	}

	// Write to file
	outputFilePath := fmt.Sprintf(outputPath, warehouse)
	outputFile, err := os.Open(outputFilePath)
	if err != nil {
		return err
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, output)
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
