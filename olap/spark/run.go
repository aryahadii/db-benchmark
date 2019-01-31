// #!/usr/bin/env gorun

package run

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
)

const (
	containerName = "spark-master"
)

var (
	dataFormat string
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

	dataFormat = os.Args[1]
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
	return runSparkShell(queryFilePath)
}

func runSparkShell(queryFilePath string) error {
	queryFileName := path.Base(queryFilePath)
	psqlArgs := []string{
		"exec",
		"-i",
		containerName,
		"sh",
		"-c",
		fmt.Sprintf(
			"spark-submit --py-files loader.py --packages com.databricks:spark-avro_2.11:4.0.0 < %s",
			queryFileName,
		),
	}
	psqlCmd := exec.Command("docker", psqlArgs...)
	output, err := psqlCmd.CombinedOutput()

	outputFilePath := queryFilePath + ".out"
	ioutil.WriteFile(outputFilePath, output, os.ModePerm)
	return err
}

func copyFileToContainerRoot(filePath, containerName) error {
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	substituted := []byte(strings.Replace(string(file), "{}", dataFormat, -1))
	newFilePath := fmt.Sprintf("%s.%s", filePath, dataFormat)
	if err := ioutil.WriteFile(newFilePath, substituted, 0644); err != nil {
		return err
	}

	copyArgs := []string{
		"cp",
		newFilePath,
		fmt.Sprintf("%s:/%s", containerName, filepath),
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
