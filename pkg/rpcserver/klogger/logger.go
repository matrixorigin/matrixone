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

package klogger

import (
	"context"
	"fmt"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
	"io"
)

func New() *klogger {
	return &klogger{}
}

func (kl *klogger) SetOutput(w io.Writer) {
	//TODO
}

func (kl *klogger) SetLevel(lv klog.Level) {
	//TODO
}

func (kl *klogger) Fatal(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Fatal(msg)
}

func (kl *klogger) Error(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Error(msg)
}

func (kl *klogger) Warn(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg)
}

// kitex have notice level, but zap don't have the level, so we think notice level == warn level
func (kl *klogger) Notice(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg)
}

func (kl *klogger) Info(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Info(msg)
}

func (kl *klogger) Debug(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg)
}

// kitex have trace level, but zap don't have the level, so we think trace level == debug level
func (kl *klogger) Trace(v ...interface{}) {
	msg := fmt.Sprint(v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg)
}

func (kl *klogger) Fatalf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Fatal(msg)
}

func (kl *klogger) Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Error(msg)
}

func (kl *klogger) Warnf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg)
}

// kitex have notice level, but zap don't have the level, so we think notice level == warn level
func (kl *klogger) Noticef(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg)
}

func (kl *klogger) Infof(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Info(msg)
}

func (kl *klogger) Debugf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg)
}

// kitex have trace level, but zap don't have the level, so we think trace level == debug level
func (kl *klogger) Tracef(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg)
}

func (kl *klogger) CtxFatalf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Fatal(msg)
}

func (kl *klogger) CtxErrorf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Error(msg)
}

func (kl *klogger) CtxWarnf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg)
}

// kitex have notice level, but zap don't have the level, so we think notice level == warn level
func (kl *klogger) CtxNoticef(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Warn(msg)
}

func (kl *klogger) CtxInfof(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Info(msg)
}

func (kl *klogger) CtxDebugf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg)
}

// kitex have trace level, but zap don't have the level, so we think trace level == debug level
func (kl *klogger) CtxTracef(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	logutil.GetGlobalLogger().WithOptions(zap.AddCallerSkip(1)).Debug(msg)
}
