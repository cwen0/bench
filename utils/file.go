package utils

import (
	"fmt"
	"os"
)

func CreateDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.Mkdir(path, 0777)
		if err != nil {
			return fmt.Errorf("Mkdir %s Error: %s", path, err)
		}
	}
	return nil
}

func CreateFile(dir, filename string) (*os.File, error) {
	err := CreateDir(dir)
	if err != nil {
		return nil, err
	}

	filePath := dir + "/" + filename
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("Create %s Error: %s", filePath, err)
	}
	return file, nil
}
