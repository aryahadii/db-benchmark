// #!/usr/bin/env gorun

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

const (
	StorageFormatParquet = iota
	StorageFormatORC
	StorageFormatAvro
)

const (
	sparkMasterContainer = "spark-master"
	loaderFileName       = "loader.py"
)

var (
	scripts = map[int]string{
		0: "loader_parquet.py",
		1: "loader_orc.py",
		2: "loader_avro.py",
	}
)

func main() {
	formats := parseArgs()
	loadData(formats)
}

func parseArgs() []int {
	if len(os.Args) < 2 {
		log.Fatalln("not enough args")
	}

	storageFormats := []int{}
	for _, arg := range os.Args[1:] {
		if arg == "parquet" {
			storageFormats = append(storageFormats, StorageFormatParquet)
		} else if arg == "orc" {
			storageFormats = append(storageFormats, StorageFormatORC)
		} else if arg == "avro" {
			storageFormats = append(storageFormats, StorageFormatAvro)
		}
	}
	return storageFormats
}

func loadData(formats []int) {
	copyFileToContainerRoot(loaderFileName, sparkMasterContainer)
	for _, format := range formats {
		scriptFileName := scripts[format]
		copyFileToContainerRoot(scriptFileName, sparkMasterContainer)
		if err := submitScript(scriptFileName); err != nil {
			log.Printf("error occured while running <%s>: %v", scriptFileName, err)
		}
	}
}

func submitScript(scriptPath string) error {
	args := []string{
		"exec",
		"-i",
		sparkMasterContainer,
		"bash",
		"-c",
		fmt.Sprintf("./bin/spark-submit --py-files loader.py --packages com.databricks:spark-avro_2.11:4.0.0 %s", scriptPath),
	}
	submitCmd := exec.Command("docker", args...)
	output, err := submitCmd.CombinedOutput()

	outputFilePath := scriptPath + ".out"
	ioutil.WriteFile(outputFilePath, output, os.ModePerm)
	return err
}

func copyFileToContainerRoot(fileName, containerName string) error {
	copyArgs := []string{
		"cp",
		fileName,
		containerName + ":/",
	}
	copyCmd := exec.Command("docker", copyArgs...)
	copyCmd.CombinedOutput()
	return nil
}
