//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package runtime

import (
	"fmt"
	"os"
	"syscall"
)

func lockJournal(file *os.File) error {
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("journal is already in use: %w", err)
	}
	return nil
}
