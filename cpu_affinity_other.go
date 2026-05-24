//go:build !linux

package gogo

func pinCurrentOSThreadToCPUIndex(idx int) (int, error) {
	return -1, nil
}
