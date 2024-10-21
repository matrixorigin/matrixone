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

package perfcounter

import (
	"context"
	"fmt"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

func Update(ctx context.Context, fn func(*CounterSet), extraCounterSets ...*CounterSet) {
	//checkStack3()
	var counterSets CounterSets

	//if counter, ok := ctx.Value(S3RequestKey{}).(*CounterSet); ok && counter != nil {
	//	fn(counter)
	//}

	// Check if InternalExecutorKey is present in the context.
	if ctx.Value(InternalExecutorKey{}) != nil {
		// No action is taken when InternalExecutorKey is present.
		// Not processing for the time being
	} else {
		// If the InternalExecutorKey does not exist, it means that you are using a generic executor.
		if counter1, ok := ctx.Value(ExecPipelineMarkKey{}).(*CounterSet); ok && counter1 != nil {
			// No code here; At this stage, independent functions are used to statistically analyze S3 requests
		} else if counter2, ok := ctx.Value(CompilePlanMarkKey{}).(*CounterSet); ok && counter2 != nil {
			fn(counter2)
		} else if counter3, ok := ctx.Value(BuildPlanMarkKey{}).(*CounterSet); ok && counter3 != nil {
			fn(counter3)
		}

		// Handling the usage of S3 resources when calling a function
		if counter, ok := ctx.Value(S3RequestKey{}).(*CounterSet); ok && counter != nil {
			fn(counter)
		}
	}

	// from context
	v := ctx.Value(CtxKeyCounters)
	if v != nil {
		counterSets = v.(CounterSets)
		for set := range counterSets {
			fn(set)
		}
	}

	// extra
	for _, set := range extraCounterSets {
		if set == nil {
			continue
		}
		if counterSets != nil {
			if _, ok := counterSets[set]; ok {
				continue
			}
		}
		fn(set)
	}

	// global
	if _, ok := counterSets[globalCounterSet]; !ok {
		fn(globalCounterSet)
	}

	// per table TODO
}

const maxStacks = 2 // 设置达到的堆栈数量后打印
const maxLines = 60 // 设置evalExpression相关堆栈的最大打印行数

var (
	stackCollection = make(map[string]struct{}) // 全局变量，存储筛选的堆栈信息
	mu              sync.Mutex                  // 保护 stackCollection 的互斥锁
	lastPrintTime   time.Time                   // 上次打印的时间
	printInterval   = 3 * time.Minute           // 打印间隔
)

// 用于匹配地址信息和参数的正则表达式
var hexRegex = regexp.MustCompile(`\s*0x\w+.*$`) // 匹配以0x开头的字符串

func cleanStackLine(line string) string {
	// 去掉以0x开头的后续字符串
	line = hexRegex.ReplaceAllString(line, "")
	return strings.TrimSpace(line) // 返回清理后的行
}
func checkStack() {
	stackInfo := string(debug.Stack())

	// 检查是否包含 'pkg/sql/colexec'
	if strings.Contains(stackInfo, "pkg/sql/colexec") {
		stackLines := strings.Split(stackInfo, "\n")

		for j, line := range stackLines {
			if strings.Contains(line, "pkg/sql/colexec") {
				// 保存当前行及后面尽可能的5行到集合中
				var selectedLines []string
				end := j + 6 // 当前行和后面的5行
				if end > len(stackLines) {
					end = len(stackLines) // 处理不足5行的情况
				}
				for _, selectedLine := range stackLines[j:end] {
					cleanedLine := cleanStackLine(selectedLine)
					selectedLines = append(selectedLines, cleanedLine)
				}

				cleanedStack := strings.Join(selectedLines, "\n")

				mu.Lock() // 加锁
				// 直接存储到 map 进行去重
				stackCollection[cleanedStack] = struct{}{}
				mu.Unlock() // 解锁
				break
			}
		}
	}

	mu.Lock()
	// 检查是否达到打印间隔
	if time.Since(lastPrintTime) >= printInterval && len(stackCollection) >= 2 {
		for stackInfo := range stackCollection {
			fmt.Printf("------------------------wuxiliang6-------------------Function Call Stack:%s\n----------------------------------------------------\n", string(stackInfo))
		}
		// 清空集合
		stackCollection = make(map[string]struct{}) // 清空已存在的堆栈信息
		lastPrintTime = time.Now()                  // 更新最后打印时间
	}
	mu.Unlock()
}

func checkStack2() {
	stackInfo := string(debug.Stack())
	stackLines := strings.Split(stackInfo, "\n") // 按行拆分堆栈

	// 处理包含 /pkg/sql/colexec/evalExpression.go 的情况，从第 0 行开始截取
	if strings.Contains(stackInfo, "/pkg/sql/colexec/evalExpression.go") {
		var selectedLines []string
		end := maxLines // 最多获取30行
		if end > len(stackLines) {
			end = len(stackLines) // 如果不足30行，获取所有行
		}
		// 从第 0 行开始获取，并进行清理
		for _, selectedLine := range stackLines[:end] {
			cleanedLine := cleanStackLine(selectedLine)
			selectedLines = append(selectedLines, cleanedLine)
		}
		cleanedStack := strings.Join(selectedLines, "\n")

		mu.Lock()
		stackCollection[cleanedStack] = struct{}{} // 添加到集合中
		mu.Unlock()
	}

	if strings.Contains(stackInfo, "pkg/sql/colexec/output") {
		var selectedLines []string
		end := maxLines // 最多获取30行
		if end > len(stackLines) {
			end = len(stackLines) // 如果不足30行，获取所有行
		}
		// 从第 0 行开始获取，并进行清理
		for _, selectedLine := range stackLines[:end] {
			cleanedLine := cleanStackLine(selectedLine)
			selectedLines = append(selectedLines, cleanedLine)
		}
		cleanedStack := strings.Join(selectedLines, "\n")

		mu.Lock()
		stackCollection[cleanedStack] = struct{}{} // 添加到集合中
		mu.Unlock()
	}

	mu.Lock()
	// 检查是否达到打印间隔
	if time.Since(lastPrintTime) >= printInterval && len(stackCollection) >= 2 {
		for stackInfo := range stackCollection {
			fmt.Printf("------------------------wuxiliang6-------------------Function Call Stack:%s\n----------------------------------------------------\n", string(stackInfo))
		}
		// 清空集合
		stackCollection = make(map[string]struct{}) // 清空已存在的堆栈信息
		lastPrintTime = time.Now()                  // 更新最后打印时间
	}
	mu.Unlock()
}

func checkStack3() {
	stackInfo := string(debug.Stack())

	// 检查是否包含 'pkg/sql/colexec'
	if strings.Contains(stackInfo, "pkg/sql/compile/") {
		stackLines := strings.Split(stackInfo, "\n")

		for j, line := range stackLines {
			if strings.Contains(line, "pkg/sql/compile/") {
				// 保存当前行及后面尽可能的5行到集合中
				var selectedLines []string
				end := j + 6 // 当前行和后面的5行
				if end > len(stackLines) {
					end = len(stackLines) // 处理不足5行的情况
				}

				start := j - 6
				if start <= 0 {
					start = 0
				}
				for _, selectedLine := range stackLines[start:end] {
					cleanedLine := cleanStackLine(selectedLine)
					selectedLines = append(selectedLines, cleanedLine)
				}

				cleanedStack := strings.Join(selectedLines, "\n")

				mu.Lock() // 加锁
				// 直接存储到 map 进行去重
				stackCollection[cleanedStack] = struct{}{}
				mu.Unlock() // 解锁
				break
			}
		}
	}

	mu.Lock()
	// 检查是否达到打印间隔
	if time.Since(lastPrintTime) >= printInterval && len(stackCollection) >= 3 {
		for stackInfo := range stackCollection {
			fmt.Printf("------------------------wuxiliang6-------------------Function Call Stack:%s\n----------------------------------------------------\n", string(stackInfo))
		}
		// 清空集合
		stackCollection = make(map[string]struct{}) // 清空已存在的堆栈信息
		lastPrintTime = time.Now()                  // 更新最后打印时间
	}
	mu.Unlock()
}
