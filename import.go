package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	searchDir := "/home/ubuntu/rawdata/aviation/airline_ontime"
	unzipDestination := "/home/ubuntu/tmp"
	filteredCSVDest := "/home/ubuntu/data"

	fileList := []string{}
	fileNames := []string{}
	filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		fmt.Println(path)
		fmt.Println(info.Mode())
		if !info.IsDir() {
			//fmt.Println("INFO:", path)
			fileList = append(fileList, path)
			fileNames = append(fileNames, info.Name())
		}
		return nil
	})

	for i, file := range fileList {
		//fmt.Println("INFO: Unzipping file :", file)
		//fmt.Println("INFO: file name", fileNames[i])
		err := unzip(file, unzipDestination)
		if err != nil {
			fmt.Println("ERROR:", err)
			continue
		}
		fmt.Println("INFO:", file)
		destFileName := fmt.Sprintf("On_Time_Filtered_%04d", i)
		csvFileName := strings.Replace(fileNames[i], ".zip", ".csv", -1)
		csvDestPath := filteredCSVDest + "/" + destFileName + ".csv"
		csvSourcePath := unzipDestination + "/" + csvFileName
		err = filterAndWriteCSV(csvSourcePath, csvDestPath)
		if err != nil {
			fmt.Println("ERROR:", err)
		}
		fmt.Println("INFO: csv dest file:", csvDestPath)
		fmt.Println("INFO: Removing file:", csvSourcePath)
		err = os.Remove(csvSourcePath)
		if err != nil {
			fmt.Println("WARN:", err)
		}
	}
}

func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func filterAndWriteCSV(source, dest string) error {
	//source := "/Users/ubuntu/data/On_Time_On_Time_Performance_2008_1.csv"
	const (
		FlightDate    = 5
		UniqueCarrier = 6
		FlightNum     = 10
		Origin        = 11
		Dest          = 17
		DepTime       = 24
		DepDelay		  = 25
		ArrTime       = 35
		ArrDelay		  = 36
		Cancelled     = 41
		Diverted      = 43
	)

	csvFile, err := os.Open(source)
	if err != nil {
		// err is printable
		// elements passed are separated by space automatically
		fmt.Println("Error: Cannot open CSV file", err)
		return err
	}

	// automatically call Close() at the end of current method
	defer csvFile.Close()

	filteredCSVFile, err := os.Create(dest)
	if err != nil {
		fmt.Println("ERROR: Cannot create file", err)
		return err
	}
	defer filteredCSVFile.Close()

	writer := csv.NewWriter(filteredCSVFile)

	reader := csv.NewReader(bufio.NewReader(csvFile))

	// eliminate the header row
	reader.Read()
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error: cannot read line from csv file ", err)
		}
		record := []string{
			line[FlightDate], line[UniqueCarrier], line[FlightNum],
			line[Origin], line[Dest], line[DepTime], line[DepDelay],
			line[ArrTime], line[ArrDelay], line[Cancelled], line[Diverted],
		}

		defer writer.Flush()
		errWrite := writer.Write(record)
		if errWrite != nil {
			fmt.Println("Error: Cannot write to csv file", errWrite)
		}
	}
	return nil
}
