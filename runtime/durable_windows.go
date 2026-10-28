package runtime

import (
	"os"
	"syscall"
	"unsafe"
)

// Windows has no portable directory fsync. File contents are flushed before
// replacement; MoveFileExW WRITE_THROUGH commits the rename before returning.
// This is a local filesystem protocol, not a claim about faulty hardware.
func syncDirectory(path string) error { return nil }

func replaceDurable(source, target string) error {
	from, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	move := syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")
	ok, _, callErr := move.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)), 0x1|0x8)
	if ok == 0 {
		return &os.LinkError{Op: "MoveFileExW", Old: source, New: target, Err: callErr}
	}
	return nil
}
