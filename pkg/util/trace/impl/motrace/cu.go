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

// DecimalNSThreshold default 100 sec
const DecimalNSThreshold = 100e9

func CalculateCUWithCfg(stats statistic.StatsArray, durationNS int64, cfg *config.OBCUConfig) float64 {

	if cfg == nil {
		cfg = GetCUConfig()
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
	cfg := GetCUConfig()
	return CalculateCUWithCfg(stats, durationNS, cfg)
}

// CalculateCUv1 calculate CU cost
func CalculateCUv1(stats statistic.StatsArray, durationNS int64) float64 {
	cfg := GetCUConfigV1()
	return CalculateCUWithCfg(stats, durationNS, cfg)
}

// CalculateCUCpu
// Be careful of the number overflow
func CalculateCUCpu(cpuRuntimeNS int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cfg = GetCUConfig()
	}
	return float64(cpuRuntimeNS) / cfg.CUUnit * cfg.CpuPrice
}

// CalculateCUMem
// Be careful of the number overflow
func CalculateCUMem(memByte int64, durationNS int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cfg = GetCUConfig()
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
	price := mustDecimal128(convertFloat64ToDecimal128Price(memPrice))
	unit := mustDecimal128(convertFloat64ToDecimal128Price(cuUnit))

	val1 := types.Decimal256FromInt64(memByte)
	val2 := types.Decimal256FromInt64(durationNS)
	val3 := types.Decimal256FromDecimal128(price)
	val4 := types.Decimal256FromDecimal128(unit)
	cuScale := int32(Decimal128ScalePrice)
	var err error
	val1, err = val1.Mul256(val2)
	if err != nil {
		return 0, err
	}
	val1, err = val1.Mul256(val3)
	if err != nil {
		return 0, err
	}
	val1, err = val1.Scale(cuScale)
	if err != nil {
		return 0, err
	}
	val1, err = val1.Div256(val4)
	if err != nil {
		return 0, err
	}

	return types.Decimal256ToFloat64(val1, cuScale), nil
}

func CalculateCUIOIn(ioCnt int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cfg = GetCUConfig()
	}
	return float64(ioCnt) * cfg.IoInPrice / cfg.CUUnit
}

func CalculateCUIOOut(ioCnt int64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cfg = GetCUConfig()
	}
	return float64(ioCnt) * cfg.IoOutPrice / cfg.CUUnit
}

func CalculateCUTraffic(bytes int64, connType float64, cfg *config.OBCUConfig) float64 {
	if cfg == nil {
		cfg = GetCUConfig()
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

var cuCfg = config.NewOBCUConfig()
var cuCfgV1 = config.NewOBCUConfig()

func GetCUConfig() *config.OBCUConfig {
	return cuCfg
}

func GetCUConfigV1() *config.OBCUConfig {
	return cuCfgV1
}

func SetCuConfig(cu, cuv1 *config.OBCUConfig) {
	cuCfg, cuCfgV1 = cu, cuv1
}
