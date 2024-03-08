// Copyright 2024 Matrix Origin
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

package motrace

import (
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"

	"go.uber.org/zap"
)

const DecimalNSThreshold = 1e9

func CalculateCUWithCfg(stats statistic.StatsArray, durationNS int64, cfg *config.OBCUConfig) float64 {

	if cfg == nil {
		cuCfg := GetTracerProvider().GetCUConfig()
		cfg = &cuCfg
	}

	// basic
	cpu := stats.GetTimeConsumed() * cfg.CpuPrice
	ioIn := stats.GetS3IOInputCount() * cfg.IoInPrice
	ioOut := stats.GetS3IOOutputCount() * cfg.IoOutPrice
	connType := stats.GetConnType()
	traffic := 0.0
	switch statistic.ConnType(connType) {
	case statistic.ConnTypeUnknown:
		traffic = stats.GetOutTrafficBytes() * cfg.TrafficPrice0
	case statistic.ConnTypeInternal:
		traffic = stats.GetOutTrafficBytes() * cfg.TrafficPrice1
	case statistic.ConnTypeExternal:
		traffic = stats.GetOutTrafficBytes() * cfg.TrafficPrice2
	default:
		logutil.Warn("Unknown ConnType as CU", zap.Float64("connType", connType))
	}

	if durationNS <= DecimalNSThreshold {
		mem := stats.GetMemorySize() * float64(durationNS) * cfg.MemPrice
		return (cpu + mem + ioIn + ioOut + traffic) / cfg.CUUnit
	} else {
		memCU := CalculateCUMem(int64(stats.GetMemorySize()), durationNS, cfg)
		return (cpu+ioIn+ioOut+traffic)/cfg.CUUnit + memCU

	}
}

// CalculateCU calculate CU cost
// the result only keep 3 decimal places
// Tips: CU is long-tailed numbers
func CalculateCU(stats statistic.StatsArray, durationNS int64) float64 {
	cfg := GetTracerProvider().GetCUConfig()
	return CalculateCUWithCfg(stats, durationNS, &cfg)
}

// CalculateCUv1 calculate CU cost
func CalculateCUv1(stats statistic.StatsArray, durationNS int64) float64 {
	cfg := config.GetOBCUConfigV1()
	return CalculateCUWithCfg(stats, durationNS, &cfg)
}

// CalculateCUCpu
// Be careful of the number overflow
func CalculateCUCpu(cpuRuntimeNS int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cuCfg := GetTracerProvider().GetCUConfig()
		cfg = &cuCfg
	}
	return float64(cpuRuntimeNS) / cfg.CUUnit * cfg.CpuPrice
}

// CalculateCUMem
// Be careful of the number overflow
func CalculateCUMem(memByte int64, durationNS int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cuCfg := GetTracerProvider().GetCUConfig()
		cfg = &cuCfg
	}
	if durationNS > DecimalNSThreshold {
		val, err := CalculateCUMemDecimal(memByte, durationNS, cfg.MemPrice, cfg.CUUnit)
		if err == nil {
			return val
		}
	}
	// float64 has 52 bits for number, so it can represent 15 digits valid number
	return float64(durationNS) / cfg.CUUnit * cfg.MemPrice * float64(memByte)
}

const Decimal128ScalePrice = 26

func convertFloat64ToDecimal128Price(val float64) (types.Decimal128, error) {
	return types.Decimal128FromFloat64(val, Decimal128Width, Decimal128ScalePrice)
}

func CalculateCUMemDecimal(memByte, durationNS int64, memPrice, cuUnit float64) (float64, error) {
	bytes := mustDecimal128(convertFloat64ToDecimal128(float64(memByte)))
	ns := mustDecimal128(convertFloat64ToDecimal128(float64(durationNS)))
	price := mustDecimal128(convertFloat64ToDecimal128Price(memPrice))
	unit := mustDecimal128(convertFloat64ToDecimal128Price(cuUnit))

	val1, valScale1, err := bytes.Mul(ns, Decimal128Scale, Decimal128Scale)
	if err != nil {
		return 0, err
	}
	val2, valScale2, err := val1.Mul(price, valScale1, Decimal128ScalePrice)
	if err != nil {
		return 0, err
	}
	cuVal, cuScale, err := val2.Div(unit, valScale2, Decimal128ScalePrice)
	if err != nil {
		return 0, err
	}

	// adjust scale
	if cuScale > statistic.Float64PrecForCU {
		//cuVal.Round(cuScale, statistic.Float64PrecForCU)
		val, err := cuVal.Scale(statistic.Float64PrecForCU - cuScale)
		if err == nil {
			cuVal = val
			cuScale = statistic.Float64PrecForCU
		}
	}

	return types.Decimal128ToFloat64(cuVal, cuScale), nil
}

func CalculateCUIOIn(ioCnt int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cuCfg := GetTracerProvider().GetCUConfig()
		cfg = &cuCfg
	}
	return float64(ioCnt) * cfg.IoInPrice / cfg.CUUnit
}

func CalculateCUIOOut(ioCnt int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cuCfg := GetTracerProvider().GetCUConfig()
		cfg = &cuCfg
	}
	return float64(ioCnt) * cfg.IoOutPrice / cfg.CUUnit
}

func CalculateCUTraffic(bytes int64, connType float64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cuCfg := GetTracerProvider().GetCUConfig()
		cfg = &cuCfg
	}
	traffic := 0.0
	switch statistic.ConnType(connType) {
	case statistic.ConnTypeUnknown:
		traffic = float64(bytes) * cfg.TrafficPrice0
	case statistic.ConnTypeInternal:
		traffic = float64(bytes) * cfg.TrafficPrice1
	case statistic.ConnTypeExternal:
		traffic = float64(bytes) * cfg.TrafficPrice2
	default:
		traffic = float64(bytes) * cfg.TrafficPrice0
	}
	return traffic / cfg.CUUnit
}
