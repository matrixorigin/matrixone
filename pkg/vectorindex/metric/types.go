package metric

import "gonum.org/v1/gonum/mat"

type MetricType uint16

const (
	Metric_L2Distance MetricType = iota
	Metric_InnerProduct
	Metric_CosineDistance
	Metric_L1Distance

	Metric_TypeCount
)

var (
	DistFuncOpTypes = map[string]string{
		"l2_distance":     "vector_l2_ops",
		"inner_product":   "vector_ip_ops",
		"cosine_distance": "vector_cosine_ops",
	}
	DistFuncInternalDistFunc = map[string]string{
		"l2_distance":     "l2_distance_sq",
		"inner_product":   "inner_product",
		"cosine_distance": "cosine_distance",
	}

	DistTypeStrToEnum = map[string]MetricType{
		"vector_l2_ops":     Metric_L2Distance,
		"vector_ip_ops":     Metric_InnerProduct,
		"vector_cosine_ops": Metric_CosineDistance,
		"vector_l1_ops":     Metric_L1Distance,
	}
)

// DistanceFunction is a function that computes the distance between two vectors
// NOTE: clusterer already ensures that the all the input vectors are of the same length,
// so we don't need to check for that here again and return error if the lengths are different.
type DistanceFunction func(v1, v2 *mat.VecDense) float64
