//go:build linux

package gogo

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func pinCurrentOSThreadToCPUIndex(idx int) (int, error) {
	var allowed unix.CPUSet
	if err := unix.SchedGetaffinity(0, &allowed); err != nil {
		return -1, err
	}
	count := allowed.Count()
	if count == 0 {
		return -1, fmt.Errorf("current thread has an empty CPU affinity mask")
	}

	ordinal := idx % count
	for cpu := 0; cpu < 4096; cpu++ {
		if !allowed.IsSet(cpu) {
			continue
		}
		if ordinal > 0 {
			ordinal--
			continue
		}

		var target unix.CPUSet
		target.Zero()
		target.Set(cpu)
		if err := unix.SchedSetaffinity(0, &target); err != nil {
			return -1, err
		}
		return cpu, nil
	}

	return -1, fmt.Errorf("could not find CPU %d in affinity mask with %d CPUs", idx, count)
}
