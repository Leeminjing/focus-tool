package runtime

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Closing the handle (including process death) releases the kernel lock.
// https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-lockfileex
func lockJournal(file *os.File) error {
	var overlapped syscall.Overlapped
	lock := syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")
	ok, _, err := lock.Call(file.Fd(), 3, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if ok == 0 {
		return fmt.Errorf("journal is already in use: %w", err)
	}
	return nil
}
