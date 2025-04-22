#!/bin/bash

count=0
temp_log=$(mktemp)
trap 'rm -f "$temp_log"' EXIT

# 新增失败日志记录
mkdir -p failed_logs

while true; do
    ((count++))
    echo -e "\n\033[34m=== 运行测试尝试 #$count ===\033[0m"
    
    # 添加测试参数 -count=1 禁用缓存
    go test -race -count=1 -run TestDoHeartbeatInvalidTask 2>&1 | tee "$temp_log"
    test_exit_code=${PIPESTATUS[0]}
    
    # 保存所有失败日志（即使没有竞态）
    if [ $test_exit_code -ne 0 ]; then
        cp "$temp_log" "failed_logs/attempt_${count}.log"
        echo "测试失败日志已保存: failed_logs/attempt_${count}.log"
    fi
    
    # 修正后的竞态检测（去掉行首匹配）
    if grep -q "WARNING: DATA RACE" "$temp_log"; then
        echo -e "\n\033[31m〓 在第 $count 次尝试检测到数据竞争 〓\033[0m"
        cp "$temp_log" "race_attempt_$count.log"
        echo "完整竞态日志已保存到: race_attempt_$count.log"
        exit 0
    fi
done
