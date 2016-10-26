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
