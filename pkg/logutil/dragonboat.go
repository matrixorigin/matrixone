package logutil

import (
	"github.com/lni/dragonboat/v4/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"sync/atomic"
)

var gZapConfigs atomic.Value

type ZapConfig struct {
	enc zapcore.Encoder
	out zapcore.WriteSyncer
}
type ZapConfigs struct {
	cfgs    []ZapConfig
	options []zap.Option
}

var _ logger.ILogger = (*DragonboatAdaptLogger)(nil)

type DragonboatAdaptLogger struct {
	logger  *zap.SugaredLogger
	atom    *zap.AtomicLevel
	pkgName string
}

func (d *DragonboatAdaptLogger) SetLevel(level logger.LogLevel) {
	switch level {
	case logger.CRITICAL:
		d.atom.SetLevel(zapcore.DPanicLevel)
	case logger.ERROR:
		d.atom.SetLevel(zapcore.ErrorLevel)
	case logger.WARNING:
		d.atom.SetLevel(zapcore.WarnLevel)
	case logger.INFO:
		d.atom.SetLevel(zapcore.InfoLevel)
	case logger.DEBUG:
		d.atom.SetLevel(zapcore.DebugLevel)
	default:
		d.atom.SetLevel(zapcore.DebugLevel)
	}
}

func (d DragonboatAdaptLogger) Debugf(format string, args ...interface{}) {
	d.logger.Debugf(format, args...)
}

func (d DragonboatAdaptLogger) Infof(format string, args ...interface{}) {
	d.logger.Infof(format, args...)
}

func (d DragonboatAdaptLogger) Warningf(format string, args ...interface{}) {
	d.logger.Warnf(format, args...)
}

func (d DragonboatAdaptLogger) Errorf(format string, args ...interface{}) {
	d.logger.Errorf(format, args...)
}

func (d DragonboatAdaptLogger) Panicf(format string, args ...interface{}) {
	d.logger.Panicf(format, args...)
}

// DragonboatFactory implement logger.Factory for logger.SetLoggerFactory
// create DragonboatAdaptLogger intance
func DragonboatFactory(pkgName string) logger.ILogger {
	var cores []zapcore.Core
	configs := gZapConfigs.Load().(*ZapConfigs)
	atom := zap.NewAtomicLevel()
	for _, cfg := range configs.cfgs {
		cores = append(cores, zapcore.NewCore(cfg.enc, cfg.out, atom))
	}
	return &DragonboatAdaptLogger{
		logger:  zap.New(zapcore.NewTee(cores...)).WithOptions(zap.AddCallerSkip(2)).Sugar(),
		atom:    &atom,
		pkgName: pkgName,
	}
}

func init() {
	logger.SetLoggerFactory(DragonboatFactory)
}
