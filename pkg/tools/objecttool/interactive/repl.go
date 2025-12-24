// Copyright 2021 Matrix Origin
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

package interactive

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
)

// Run 运行交互式界面
func Run(path string) error {
	ctx := context.Background()

	// 1. 打开object
	reader, err := objecttool.Open(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to open object: %w", err)
	}
	defer reader.Close()

	// 2. 初始化状态
	state := NewState(ctx, reader)
	defer state.Close()

	view := NewView()

	// 3. 主循环（行模式）
	return runLineMode(state, view)
}

// runLineMode 行模式
func runLineMode(state *State, view *View) error {
	scanner := bufio.NewScanner(os.Stdin)
	var message string

	for {
		// 渲染界面
		if err := view.Render(state, message); err != nil {
			return err
		}
		message = ""

		// 显示提示符
		fmt.Print(":")

		// 读取输入
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return err
			}
			// EOF
			return nil
		}

		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		// 解析命令
		cmd, err := ParseCommand(input)
		if err != nil {
			message = fmt.Sprintf("Error: %v", err)
			continue
		}

		if cmd == nil {
			continue
		}

		// 执行命令
		output, quit, err := cmd.Execute(state)
		if err != nil {
			message = fmt.Sprintf("Error: %v", err)
			continue
		}

		if quit {
			return nil
		}

		if output != "" {
			message = output
		}
	}
}
