//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package runtime

import (
	"os"
	"path/filepath"
)

func syncDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func replaceDurable(source, target string) error {
	if err := os.Rename(source, target); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(target))
}
