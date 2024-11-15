package main

import (
	"fmt"
	"os"
	"time"

	"math/rand"
)

// Create the file if it does not exists, or truncate the
// existing one before write the content.
func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err

	}

	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}

	return fp.Sync()

}

// Saves data to a file by writing it to a temporary file first and then
// renaming it to the final path.
func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tmp)
		}
	}()
	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	err = fp.Sync() // fsync
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func randomInt() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Int()
}

func main() {
	SaveData1("test.txt", []byte("Hello, World!"))
	SaveData2("test1.txt", []byte("Hello, World!"))

	SaveData1("test.txt", []byte("Hello, World 2!"))

	SaveData2("test1.txt", []byte("Hello, World 2!"))
}
