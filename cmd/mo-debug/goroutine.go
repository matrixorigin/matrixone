// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixone/pkg/util/debug/goroutine"
	"github.com/spf13/cobra"
)

var (
	parsed   []goroutine.Goroutine
	targets  []goroutine.Goroutine
	groupRes goroutine.AnalyzeResult
	analyzer = goroutine.GetAnalyzer()
)

var (
	goroutineCMD = &cobra.Command{
		Use:        "goroutine",
		Short:      "goroutine tool",
		Long:       "MO debug tool. Helps to analyze MO problems like Goroutine, Txn, etc.",
		SuggestFor: []string{"goroutine"},
		Run:        handleGoroutineCommand,
	}
)

func handleGoroutineCommand(
	cmd *cobra.Command,
	args []string) {

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf(">>> ")
		if scanner.Scan() {
			line := strings.ToLower(strings.TrimSpace(scanner.Text()))
			if strings.HasPrefix(line, "parse") {
				handleParse(line)
			} else if strings.HasPrefix(line, "filter") {
				handleFilter(line)
			} else if strings.HasPrefix(line, "top") {
				handleTop(line)
			} else if strings.HasPrefix(line, "dump") {
				handleDump(line)
			} else {
				switch line {
				case "count":
					fmt.Printf("%d\n", len(targets))
				case "reset":
					resetTargets()
				case "group":
					handleGroup()
				case "help":
					handleHelp()
				case "exit":
					return
				default:
					handleHelp()
				}
			}
			continue
		}
		return
	}
}

func handleHelp() {
	fmt.Println("command list")
	fmt.Println("\t1. parse [file|url], parse goroutines from file or url")
	fmt.Println("\t2. filter [!]str, filter goroutines which contains str, ! means not, str can be any string in goroutine's stack")
	fmt.Println("\t3. reset, drop all filter conditions")
	fmt.Println("\t4. count, show current goroutines count")
	fmt.Println("\t5. group, group goroutines")
	fmt.Println("\t6. top n [groupIndex subIndex], show top n(0 means print all groups) of group result, groupIndex and subIndex are used to control print full goroutine stack")
	fmt.Println("\t7. dump file, dump group result to file")
	fmt.Println("\t8. exit, exit.")
}

func handleParse(line string) {
	from := strings.TrimSpace(line[6:])
	fn := getFromFile
	if strings.HasPrefix(from, "http") {
		fn = getFromURL
	}

	data, err := fn(from)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	parsed = analyzer.Parse(data)
	fmt.Printf("parse %d goroutines from %s\n",
		len(parsed),
		strings.TrimSpace(line[6:]))

	resetTargets()
}

func handleFilter(line string) {
	condition := strings.TrimSpace(line[7:])
	not := condition[0] == '!'
	if not {
		condition = condition[1:]
	}

	newTargets := targets[:0]
	for _, g := range targets {
		has := g.Has(condition)
		if not {
			has = !has
		}
		if has {
			newTargets = append(newTargets, g)
		}
	}
	fmt.Printf("%d -> %d, filter by (%s)\n", len(targets), len(newTargets), line[7:])
	targets = newTargets
}

func handleTop(line string) {
	if groupRes.GroupCount() == 0 {
		fmt.Println("no group result")
		return
	}

	var fields []string
	var err error
	top := 0
	if len(line) > 4 {
		fields = strings.Split(strings.TrimSpace(line[4:]), " ")
		if len(fields) != 1 && len(fields) != 3 {
			fmt.Println("invalid top command.")
			return
		}

		top, err = format.ParseStringInt(fields[0])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	if top == 0 ||
		top >= groupRes.GroupCount() {
		top = groupRes.GroupCount()
	}

	groupIdx, subIndex := -1, -1
	if len(fields) == 3 {
		groupIdx, err = format.ParseStringInt(fields[1])
		if err != nil {
			fmt.Println(err.Error())
			return
		}

		subIndex, err = format.ParseStringInt(fields[2])
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	fmt.Println(groupRes.Display(top,
		func(i, j int) (bool, bool) {
			return false, groupIdx == i && subIndex == j
		}))
}

func handleGroup() {
	groupRes = analyzer.GroupAnalyze(targets)
	fmt.Printf("%d groups\n", groupRes.GroupCount())
}

func handleDump(line string) {
	file, err := os.OpenFile(
		strings.TrimSpace(line[4:]),
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer file.Close()

	_, err = file.WriteString(
		groupRes.Display(
			math.MaxInt,
			func(i, j int) (bool, bool) {
				return false, true
			}),
	)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func resetTargets() {
	targets = append(targets[:0], parsed...)
}

func getFromFile(file string) ([]byte, error) {
	return os.ReadFile(file)
}

func getFromURL(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil,
			fmt.Errorf("get goroutine from url failed, status code: %d", resp.StatusCode)
	}

	var buf bytes.Buffer
	_, err = io.CopyBuffer(&buf, resp.Body, make([]byte, 1024*64))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
