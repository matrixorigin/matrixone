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
	"syscall"
	"unsafe"

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

	// 3. 尝试raw模式，失败则用行模式
	if err := runRawMode(state, view); err != nil {
		// fallback到行模式
		return runLineMode(state, view)
	}
	return nil
}

// runRawMode raw模式（单字符输入）
func runRawMode(state *State, view *View) error {
	// 设置终端raw模式
	fd := int(os.Stdin.Fd())
	var oldState syscall.Termios
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCGETS, uintptr(unsafe.Pointer(&oldState))); err != 0 {
		return fmt.Errorf("failed to get terminal state: %v", err)
	}
	
	newState := oldState
	newState.Lflag &^= syscall.ICANON | syscall.ECHO
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&newState))); err != 0 {
		return fmt.Errorf("failed to set raw mode: %v", err)
	}
	
	defer func() {
		syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&oldState)))
	}()

	var message string
	buf := make([]byte, 3) // 支持方向键（ESC序列）
	cmdHistory := []string{} // 命令历史

	for {
		// 渲染界面
		if err := view.Render(state, message); err != nil {
			return err
		}
		message = ""

		// 显示提示
		fmt.Print("(j/k/Ctrl+F/B/Enter/q or : for command) ")

		// 读取单字符
		n, err := os.Stdin.Read(buf)
		if err != nil {
			return err
		}

		if n == 0 {
			continue
		}

		// 处理输入
		ch := buf[0]

		// ESC序列（方向键）
		if ch == 27 && n >= 3 {
			if buf[1] == '[' {
				switch buf[2] {
				case 'A': // 上箭头
					state.ScrollUp()
				case 'B': // 下箭头
					state.ScrollDown()
				}
			}
			continue
		}

		// 普通字符
		switch ch {
		case 'j', 'J':
			state.ScrollDown()
		case 'k', 'K':
			state.ScrollUp()
		case 'g':
			state.GotoRow(0)
		case 'G':
			state.GotoRow(-1)
		case 6: // Ctrl+F
			state.NextPage()
		case 2: // Ctrl+B
			state.PrevPage()
		case 'q', 'Q', 3: // q 或 Ctrl+C
			return nil
		case '\r', '\n': // Enter
			state.NextPage()
		case ':':
			// 进入命令模式
			cmd := handleCommandModeWithHistory(state, &cmdHistory)
			if cmd != "" {
				message = cmd
			}
		case '?':
			message = generalHelp
		}
	}
}

// handleCommandModeWithHistory 处理命令模式（支持历史和补全）
func handleCommandModeWithHistory(state *State, history *[]string) string {
	fd := int(os.Stdin.Fd())
	var oldState syscall.Termios
	syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCGETS, uintptr(unsafe.Pointer(&oldState)))
	
	normalState := oldState
	normalState.Lflag |= syscall.ICANON | syscall.ECHO
	syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&normalState)))
	
	defer func() {
		syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&oldState)))
	}()

	fmt.Print("\r\033[K:")
	
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}

	// 添加到历史
	*history = append(*history, line)

	cmd, err := ParseCommand(":" + line)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	if cmd == nil {
		return ""
	}

	output, quit, err := cmd.Execute(state)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	if quit {
		os.Exit(0)
	}

	if output != "" {
		needPause := strings.Contains(line, "info") || strings.Contains(line, "schema") || strings.Contains(line, "help")
		if needPause {
			fmt.Println()
			fmt.Println(output)
			fmt.Println()
			fmt.Print("Press any key to continue...")
			buf := make([]byte, 1)
			os.Stdin.Read(buf)
			return ""
		}
		return output
	}

	return ""
}

// handleCommandMode 处理命令模式
func handleCommandMode(state *State) string {
	// 显示命令提示符
	fmt.Print("\r\033[K:") // 清除当前行并显示 :
	
	// 临时恢复终端正常模式以显示输入
	fd := int(os.Stdin.Fd())
	var oldState syscall.Termios
	syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCGETS, uintptr(unsafe.Pointer(&oldState)))
	
	normalState := oldState
	normalState.Lflag |= syscall.ICANON | syscall.ECHO
	syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&normalState)))
	
	// 读取命令
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	
	// 恢复raw模式
	syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), syscall.TCSETS, uintptr(unsafe.Pointer(&oldState)))
	
	if err != nil {
		return fmt.Sprintf("Error reading command: %v", err)
	}

	line = strings.TrimSpace(line)
	if line == "" {
		return ""
	}

	// 解析并执行命令
	cmd, err := ParseCommand(":" + line)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	if cmd == nil {
		return ""
	}

	output, quit, err := cmd.Execute(state)
	if err != nil {
		return fmt.Sprintf("Error: %v", err)
	}

	if quit {
		os.Exit(0)
	}

	// 如果有输出，显示并等待用户按键（只对 info/schema 等查询命令）
	if output != "" {
		// 检查是否是需要暂停的命令
		needPause := strings.Contains(line, "info") || strings.Contains(line, "schema") || strings.Contains(line, "help")
		if needPause {
			fmt.Println()
			fmt.Println(output)
			fmt.Println()
			fmt.Print("Press any key to continue...")
			buf := make([]byte, 1)
			os.Stdin.Read(buf)
			return ""
		}
		// 其他命令（如 set, format, vertical）直接返回消息
		return output
	}

	return ""
}

// runLineMode 行模式
func runLineMode(state *State, view *View) error {
	scanner := bufio.NewScanner(os.Stdin)
	var message string
	commandMode := false // false=浏览模式, true=命令模式

	for {
		// 渲染界面
		if err := view.Render(state, message); err != nil {
			return err
		}
		message = ""

		// 显示提示符
		if commandMode {
			fmt.Print(":")
		} else {
			fmt.Print("(j/k/Enter/q or : for command) ")
		}

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
			if !commandMode {
				// 浏览模式下，回车=向下翻页
				state.NextPage()
			}
			continue
		}

		// 浏览模式
		if !commandMode {
			switch input {
			case "j":
				state.ScrollDown()
			case "k":
				state.ScrollUp()
			case "q":
				return nil
			case ":":
				commandMode = true
			default:
				message = "Unknown key. Press ? for help"
			}
			continue
		}

		// 命令模式
		if !strings.HasPrefix(input, ":") {
			input = ":" + input
		}

		// 解析命令
		cmd, err := ParseCommand(input)
		if err != nil {
			message = fmt.Sprintf("Error: %v", err)
			commandMode = false
			continue
		}

		if cmd == nil {
			commandMode = false
			continue
		}

		// 执行命令
		output, quit, err := cmd.Execute(state)
		if err != nil {
			message = fmt.Sprintf("Error: %v", err)
			commandMode = false
			continue
		}

		if quit {
			return nil
		}

		if output != "" {
			message = output
		}
		
		commandMode = false
	}
}
