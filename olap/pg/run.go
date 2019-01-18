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
	"path"
)

const (
	containerName = "postgres-olap_postgres-olap_1"
	databaseName  = "adb"

	RunModeExplain = iota
	RunModeValidate
	RunModeExecute
)

var (
	runMode    int
	queryFiles []string
)

func main() {
	parseArgs()
	runQueries(queryFiles)
}

func parseArgs() {
	if len(os.Args) < 3 {
		log.Fatalln("not enough args")
	}

	arg1, err := strconv.ParseInt(os.Args[1], 10, 32)
	if err != nil {
		log.Fatalln("cannot parse runMode arg")
	}
	runMode = int(arg1)

	queryFiles = os.Args[2:]
}

func runQueries(queryFilePaths []string) {
	for _, queryPath := range queryFilePaths {
		if err := runQuery(databaseName, queryPath); err != nil {
			log.Printf("error while running %s [%v]", queryPath, err)
		}
	}
}

func runQuery(databaseName, queryFilePath string) error {
	ctx, monitoringCancelFunc := context.WithCancel(context.Background())
	defer monitoringCancelFunc()
	go monitorSystem(ctx, queryFilePath)

	copyFileToContainerRoot(queryFilePath, containerName)
	return runPSQL(queryFilePath)
}

func runPSQL(queryFilePath string) error {
	queryFileName := path.Base(queryFilePath)
	psqlArgs := []string{
		"exec",
		"-i",
		containerName,
		"sh",
		"-c",
		fmt.Sprintf("psql -d %s -f /%s", databaseName, queryFileName),
	}
	psqlCmd := exec.Command("docker", psqlArgs...)
	output, err := psqlCmd.CombinedOutput()

	outputFilePath := queryFilePath + ".out"
	ioutil.WriteFile(outputFilePath, output, os.ModePerm)
	return err
}

func copyFileToContainerRoot(filePath, containerName string) error {
	copyArgs := []string{
		"cp",
		filePath,
		containerName + ":/",
	}
	copyCmd := exec.Command("docker", copyArgs...)
	copyCmd.CombinedOutput()
	return nil

}

func monitorSystem(ctx context.Context, queryFilePath string) error {
	outputFilePath := queryFilePath + ".stat"
	args := []string{
		"-lcmdrsyTt",
		"--full",
		"--output",
		outputFilePath,
	}
	return exec.CommandContext(ctx, "dstat", args...).Run()
}

