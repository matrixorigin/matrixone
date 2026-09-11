// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package udf

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// TimezoneDatabaseVersion returns the version of the system IANA tz database
// used by the current process. An IANA timezone is part of the Python UDF
// statement contract, so silently returning a made-up version would allow two
// hosts to interpret a DST boundary differently.
func TimezoneDatabaseVersion() (string, error) {
	paths := []string{
		"/usr/share/zoneinfo/tzdata.zi",
		"/usr/lib/zoneinfo/tzdata.zi",
		"/usr/share/lib/zoneinfo/tzdata.zi",
		"/etc/zoneinfo/tzdata.zi",
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		lineOK := scanner.Scan()
		line := scanner.Text()
		_ = file.Close()
		if !lineOK {
			continue
		}
		const prefix = "# version "
		if strings.HasPrefix(line, prefix) {
			version := strings.TrimSpace(strings.TrimPrefix(line, prefix))
			if version != "" {
				return version, nil
			}
		}
	}
	return "", fmt.Errorf("python udf: IANA timezone database version is unavailable")
}
