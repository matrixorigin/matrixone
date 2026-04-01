# Copyright 2021 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ctypes
import os
import numpy as np
from enum import IntEnum

# --- Library Loading ---
_lib_name = 'libmocuvs.so'
_lib_path = os.path.join(os.path.dirname(__file__), '..', _lib_name)
if not os.path.exists(_lib_path):
    _lib_path = _lib_name

try:
    _lib = ctypes.CDLL(_lib_path)
except Exception as e:
    print(f"Warning: Could not load {_lib_name} from {_lib_path}: {e}")
    _lib = None

# --- Enums ---
class DistanceType(IntEnum):
    L2Expanded = 0
    L2SqrtExpanded = 1
    CosineExpanded = 2
    L1 = 3
    L2Unexpanded = 4
    L2SqrtUnexpanded = 5
    InnerProduct = 6
    Linf = 7
    Canberra = 8
    LpUnexpanded = 9
    CorrelationExpanded = 10
    JaccardExpanded = 11
    HellingerExpanded = 12
    Haversine = 13
    BrayCurtis = 14
    JensenShannon = 15
    HammingUnexpanded = 16
    KLDivergence = 17
    RusselRaoExpanded = 18
    DiceExpanded = 19
    BitwiseHamming = 20
    Precomputed = 100
    CosineSimilarity = 2
    Jaccard = 11
    Hamming = 16
    Unknown = 255

class Quantization(IntEnum):
    F32 = 0
    F16 = 1
    INT8 = 2
    UINT8 = 3

class DistributionMode(IntEnum):
    SINGLE_GPU = 0
    SHARDED = 1
    REPLICATED = 2

# --- Parameter Structs ---
class CagraBuildParams(ctypes.Structure):
    _fields_ = [("intermediate_graph_degree", ctypes.c_size_t),
                ("graph_degree", ctypes.c_size_t),
                ("attach_dataset_on_build", ctypes.c_bool)]
    @classmethod
    def default(cls): return cls(128, 64, True)

class CagraSearchParams(ctypes.Structure):
    _fields_ = [("itopk_size", ctypes.c_size_t), ("search_width", ctypes.c_size_t)]
    @classmethod
    def default(cls): return cls(64, 1)

class IvfFlatBuildParams(ctypes.Structure):
    _fields_ = [("n_lists", ctypes.c_uint32), ("add_data_on_build", ctypes.c_bool), ("kmeans_trainset_fraction", ctypes.c_double)]
    @classmethod
    def default(cls): return cls(1024, True, 0.5)

class IvfFlatSearchParams(ctypes.Structure):
    _fields_ = [("n_probes", ctypes.c_uint32)]
    @classmethod
    def default(cls): return cls(20)

class IvfPqBuildParams(ctypes.Structure):
    _fields_ = [("n_lists", ctypes.c_uint32), ("m", ctypes.c_uint32), ("bits_per_code", ctypes.c_uint32),
                ("add_data_on_build", ctypes.c_bool), ("kmeans_trainset_fraction", ctypes.c_double)]
    @classmethod
    def default(cls): return cls(1024, 16, 8, True, 0.5)

class IvfPqSearchParams(ctypes.Structure):
    _fields_ = [("n_probes", ctypes.c_uint32)]
    @classmethod
    def default(cls): return cls(20)

# --- Result Wrapper Structs ---
class CagraSearchRes(ctypes.Structure): _fields_ = [("result_ptr", ctypes.c_void_p)]
class IvfFlatSearchRes(ctypes.Structure): _fields_ = [("result_ptr", ctypes.c_void_p)]
class IvfPqSearchRes(ctypes.Structure): _fields_ = [("result_ptr", ctypes.c_void_p)]

class KMeansFitRes(ctypes.Structure):
    _fields_ = [("inertia", ctypes.c_float), ("n_iter", ctypes.c_int64)]
class KMeansPredictRes(ctypes.Structure):
    _fields_ = [("result_ptr", ctypes.c_void_p), ("inertia", ctypes.c_float)]
class KMeansFitPredictRes(ctypes.Structure):
    _fields_ = [("result_ptr", ctypes.c_void_p), ("inertia", ctypes.c_float), ("n_iter", ctypes.c_int64)]

# --- Internal Error Handling ---
def _check_error(errmsg_ptr):
    if errmsg_ptr.value:
        msg = ctypes.string_at(errmsg_ptr.value).decode('utf-8')
        raise RuntimeError(msg)

# --- C Prototypes Setup ---
if _lib:
    # CAGRA
    _lib.gpu_cagra_new.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, CagraBuildParams, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p]
    _lib.gpu_cagra_new.restype = ctypes.c_void_p
    _lib.gpu_cagra_new_empty.argtypes = [ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, CagraBuildParams, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p]
    _lib.gpu_cagra_new_empty.restype = ctypes.c_void_p
    _lib.gpu_cagra_load_file.argtypes = [ctypes.c_char_p, ctypes.c_uint32, ctypes.c_int, CagraBuildParams, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    _lib.gpu_cagra_load_file.restype = ctypes.c_void_p
    _lib.gpu_cagra_destroy.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_cagra_start.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_cagra_build.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_cagra_add_chunk.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_cagra_add_chunk_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_cagra_train_quantizer.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_cagra_save.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_cagra_save_dir.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_cagra_load_dir.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_cagra_delete_id.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p]
    _lib.gpu_cagra_search_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_uint32, ctypes.c_uint32, CagraSearchParams, ctypes.c_void_p]
    _lib.gpu_cagra_search_float.restype = CagraSearchRes
    _lib.gpu_cagra_get_neighbors.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(ctypes.c_uint32)]
    _lib.gpu_cagra_get_distances.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(ctypes.c_float)]
    _lib.gpu_cagra_free_result.argtypes = [ctypes.c_void_p]
    _lib.gpu_cagra_len.restype = ctypes.c_uint32
    _lib.gpu_cagra_cap.restype = ctypes.c_uint32
    _lib.gpu_cagra_info.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_cagra_info.restype = ctypes.c_char_p
    _lib.gpu_cagra_merge.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_int, ctypes.c_uint32, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_void_p]
    _lib.gpu_cagra_merge.restype = ctypes.c_void_p

    # IVF-Flat
    _lib.gpu_ivf_flat_new.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, IvfFlatBuildParams, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int64), ctypes.c_void_p]
    _lib.gpu_ivf_flat_new.restype = ctypes.c_void_p
    _lib.gpu_ivf_flat_new_empty.argtypes = [ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, IvfFlatBuildParams, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int64), ctypes.c_void_p]
    _lib.gpu_ivf_flat_new_empty.restype = ctypes.c_void_p
    _lib.gpu_ivf_flat_destroy.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_ivf_flat_start.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_ivf_flat_build.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_ivf_flat_add_chunk_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_ivf_flat_save.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_ivf_flat_save_dir.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_ivf_flat_load_dir.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_ivf_flat_delete_id.argtypes = [ctypes.c_void_p, ctypes.c_int64, ctypes.c_void_p]
    _lib.gpu_ivf_flat_search_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_uint32, ctypes.c_uint32, IvfFlatSearchParams, ctypes.c_void_p]
    _lib.gpu_ivf_flat_search_float.restype = IvfFlatSearchRes
    _lib.gpu_ivf_flat_get_neighbors.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(ctypes.c_int64)]
    _lib.gpu_ivf_flat_get_distances.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(ctypes.c_float)]
    _lib.gpu_ivf_flat_free_result.argtypes = [ctypes.c_void_p]
    _lib.gpu_ivf_flat_len.restype = ctypes.c_uint32
    _lib.gpu_ivf_flat_cap.restype = ctypes.c_uint32

    # IVF-PQ
    _lib.gpu_ivf_pq_new.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, IvfPqBuildParams, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int64), ctypes.c_void_p]
    _lib.gpu_ivf_pq_new.restype = ctypes.c_void_p
    _lib.gpu_ivf_pq_new_empty.argtypes = [ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, IvfPqBuildParams, ctypes.POINTER(ctypes.c_int), ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int64), ctypes.c_void_p]
    _lib.gpu_ivf_pq_new_empty.restype = ctypes.c_void_p
    _lib.gpu_ivf_pq_destroy.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_ivf_pq_start.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_ivf_pq_build.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_ivf_pq_add_chunk_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_ivf_pq_save.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_ivf_pq_save_dir.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_ivf_pq_load_dir.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_void_p]
    _lib.gpu_ivf_pq_delete_id.argtypes = [ctypes.c_void_p, ctypes.c_int64, ctypes.c_void_p]
    _lib.gpu_ivf_pq_search_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_uint32, ctypes.c_uint32, IvfPqSearchParams, ctypes.c_void_p]
    _lib.gpu_ivf_pq_search_float.restype = IvfPqSearchRes
    _lib.gpu_ivf_pq_get_neighbors.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(ctypes.c_int64)]
    _lib.gpu_ivf_pq_get_distances.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(ctypes.c_float)]
    _lib.gpu_ivf_pq_free_result.argtypes = [ctypes.c_void_p]
    _lib.gpu_ivf_pq_len.argtypes = [ctypes.c_void_p]
    _lib.gpu_ivf_pq_len.restype = ctypes.c_uint32
    _lib.gpu_ivf_pq_cap.argtypes = [ctypes.c_void_p]
    _lib.gpu_ivf_pq_cap.restype = ctypes.c_uint32
    _lib.gpu_ivf_pq_info.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_ivf_pq_info.restype = ctypes.c_char_p

    # Brute Force
    _lib.gpu_brute_force_new.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    _lib.gpu_brute_force_new.restype = ctypes.c_void_p
    _lib.gpu_brute_force_new_empty.argtypes = [ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    _lib.gpu_brute_force_new_empty.restype = ctypes.c_void_p
    _lib.gpu_brute_force_destroy.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_brute_force_start.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_brute_force_build.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_brute_force_add_chunk_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_brute_force_search_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
    _lib.gpu_brute_force_search_float.restype = ctypes.c_void_p
    _lib.gpu_brute_force_get_results.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_float)]
    _lib.gpu_brute_force_free_search_result.argtypes = [ctypes.c_void_p]
    _lib.gpu_brute_force_len.argtypes = [ctypes.c_void_p]
    _lib.gpu_brute_force_len.restype = ctypes.c_uint32
    _lib.gpu_brute_force_cap.argtypes = [ctypes.c_void_p]
    _lib.gpu_brute_force_cap.restype = ctypes.c_uint32
    _lib.gpu_brute_force_info.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_brute_force_info.restype = ctypes.c_char_p

    # KMeans
    _lib.gpu_kmeans_new.argtypes = [ctypes.c_uint32, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_uint32, ctypes.c_int, ctypes.c_void_p]
    _lib.gpu_kmeans_new.restype = ctypes.c_void_p
    _lib.gpu_kmeans_destroy.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_kmeans_start.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_kmeans_fit.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_kmeans_fit_predict_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_kmeans_fit_predict_float.restype = KMeansFitPredictRes
    _lib.gpu_kmeans_predict_float.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_kmeans_predict_float.restype = KMeansPredictRes
    _lib.gpu_kmeans_get_labels.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.POINTER(ctypes.c_int64)]
    _lib.gpu_kmeans_free_result.argtypes = [ctypes.c_void_p]
    _lib.gpu_kmeans_get_centroids.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_kmeans_info.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    _lib.gpu_kmeans_info.restype = ctypes.c_char_p
    _lib.gpu_kmeans_predict.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint64, ctypes.c_void_p]
    _lib.gpu_kmeans_predict.restype = KMeansPredictRes

    # Utils
    _lib.gpu_get_device_count.restype = ctypes.c_int
    _lib.gpu_get_device_list.argtypes = [ctypes.POINTER(ctypes.c_int), ctypes.c_int]
    _lib.gpu_pairwise_distance.argtypes = [ctypes.c_void_p, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_float), ctypes.c_void_p]
    _lib.gpu_adhoc_brute_force_search_float.argtypes = [ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_uint32, ctypes.POINTER(ctypes.c_float), ctypes.c_uint64, ctypes.c_uint32, ctypes.c_int, ctypes.POINTER(ctypes.c_int64), ctypes.POINTER(ctypes.c_float), ctypes.c_void_p]

# --- Base Class for Shared Logic ---
class _CuvsIndexBase:
    def __init__(self, handle, destroy_func):
        self.handle = handle
        self._destroy_func = destroy_func

    def start(self):
        errmsg = ctypes.c_char_p()
        _lib.gpu_index_start(self.handle, ctypes.byref(errmsg)) if hasattr(_lib, 'gpu_index_start') else None
        # Fallback to specific start functions if generic isn't there (we handled this in subclasses)

    def __del__(self):
        if hasattr(self, 'handle') and self.handle:
            errmsg = ctypes.c_char_p()
            self._destroy_func(self.handle, ctypes.byref(errmsg))

# --- Public Classes ---

class CagraIndex:
    def __init__(self, handle): self.handle = handle

    @classmethod
    def create(cls, dataset, metric=DistanceType.L2Expanded, build_params=None, devices=[0], nthread=4, dist_mode=DistributionMode.SINGLE_GPU, qtype=Quantization.F32, ids=None):
        if build_params is None: build_params = CagraBuildParams.default()
        dataset = np.ascontiguousarray(dataset, dtype=np.float32)
        count, dim = dataset.shape
        dev_arr = (ctypes.c_int * len(devices))(*devices)
        id_ptr = ids.ctypes.data_as(ctypes.POINTER(ctypes.c_uint32)) if ids is not None else None
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_cagra_new(dataset.ctypes.data_as(ctypes.c_void_p), count, dim, int(metric), build_params, dev_arr, len(devices), nthread, int(dist_mode), int(qtype), id_ptr, ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    @classmethod
    def create_empty(cls, total_count, dimension, metric=DistanceType.L2Expanded, build_params=None, devices=[0], nthread=4, dist_mode=DistributionMode.SINGLE_GPU, qtype=Quantization.F32, ids=None):
        if build_params is None: build_params = CagraBuildParams.default()
        dev_arr = (ctypes.c_int * len(devices))(*devices)
        id_ptr = ids.ctypes.data_as(ctypes.POINTER(ctypes.c_uint32)) if ids is not None else None
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_cagra_new_empty(total_count, dimension, int(metric), build_params, dev_arr, len(devices), nthread, int(dist_mode), int(qtype), id_ptr, ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    def start(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_start(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def build(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_build(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def add_chunk(self, chunk):
        chunk = np.ascontiguousarray(chunk, dtype=np.float32)
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_add_chunk_float(self.handle, chunk.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(chunk), ctypes.byref(errmsg)); _check_error(errmsg)
    def train_quantizer(self, train_data):
        train_data = np.ascontiguousarray(train_data, dtype=np.float32)
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_train_quantizer(self.handle, train_data.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(train_data), ctypes.byref(errmsg)); _check_error(errmsg)
    def delete_id(self, id_val):
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_delete_id(self.handle, id_val, ctypes.byref(errmsg)); _check_error(errmsg)
    def save(self, filename):
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_save(self.handle, filename.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)
    def save_dir(self, directory):
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_save_dir(self.handle, directory.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)
    def load_dir(self, directory):
        errmsg = ctypes.c_char_p(); _lib.gpu_cagra_load_dir(self.handle, directory.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)

    def search(self, queries, k, search_params=None):
        if search_params is None: search_params = CagraSearchParams.default()
        queries = np.ascontiguousarray(queries, dtype=np.float32)
        num_q, dim = queries.shape
        errmsg = ctypes.c_char_p()
        res = _lib.gpu_cagra_search_float(self.handle, queries.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), num_q, dim, k, search_params, ctypes.byref(errmsg))
        _check_error(errmsg)
        neighbors = np.zeros((num_q, k), dtype=np.uint32)
        distances = np.zeros((num_q, k), dtype=np.float32)
        _lib.gpu_cagra_get_neighbors(res.result_ptr, num_q * k, neighbors.ctypes.data_as(ctypes.POINTER(ctypes.c_uint32)))
        _lib.gpu_cagra_get_distances(res.result_ptr, num_q * k, distances.ctypes.data_as(ctypes.POINTER(ctypes.c_float)))
        _lib.gpu_cagra_free_result(res.result_ptr); return neighbors, distances

    def __len__(self): return _lib.gpu_cagra_len(self.handle)
    def capacity(self): return _lib.gpu_cagra_cap(self.handle)
    def info(self):
        errmsg = ctypes.c_char_p(); s = _lib.gpu_cagra_info(self.handle, ctypes.byref(errmsg))
        _check_error(errmsg); return s.decode('utf-8') if s else ""

    def __del__(self):
        if hasattr(self, 'handle') and self.handle:
            errmsg = ctypes.c_char_p(); _lib.gpu_cagra_destroy(self.handle, ctypes.byref(errmsg))

class IvfFlatIndex:
    def __init__(self, handle): self.handle = handle

    @classmethod
    def create(cls, dataset, metric=DistanceType.L2Expanded, build_params=None, devices=[0], nthread=4, dist_mode=DistributionMode.SINGLE_GPU, qtype=Quantization.F32, ids=None):
        if build_params is None: build_params = IvfFlatBuildParams.default()
        dataset = np.ascontiguousarray(dataset, dtype=np.float32)
        count, dim = dataset.shape
        dev_arr = (ctypes.c_int * len(devices))(*devices)
        id_ptr = ids.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)) if ids is not None else None
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_ivf_flat_new(dataset.ctypes.data_as(ctypes.c_void_p), count, dim, int(metric), build_params, dev_arr, len(devices), nthread, int(dist_mode), int(qtype), id_ptr, ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    @classmethod
    def create_empty(cls, total_count, dimension, metric=DistanceType.L2Expanded, build_params=None, devices=[0], nthread=4, dist_mode=DistributionMode.SINGLE_GPU, qtype=Quantization.F32, ids=None):
        if build_params is None: build_params = IvfFlatBuildParams.default()
        dev_arr = (ctypes.c_int * len(devices))(*devices)
        id_ptr = ids.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)) if ids is not None else None
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_ivf_flat_new_empty(total_count, dimension, int(metric), build_params, dev_arr, len(devices), nthread, int(dist_mode), int(qtype), id_ptr, ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    def start(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_start(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def build(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_build(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def add_chunk(self, chunk):
        chunk = np.ascontiguousarray(chunk, dtype=np.float32)
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_add_chunk_float(self.handle, chunk.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(chunk), ctypes.byref(errmsg)); _check_error(errmsg)
    def delete_id(self, id_val):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_delete_id(self.handle, id_val, ctypes.byref(errmsg)); _check_error(errmsg)
    def save(self, filename):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_save(self.handle, filename.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)
    def save_dir(self, directory):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_save_dir(self.handle, directory.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)
    def load_dir(self, directory):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_load_dir(self.handle, directory.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)

    def search(self, queries, k, search_params=None):
        if search_params is None: search_params = IvfFlatSearchParams.default()
        queries = np.ascontiguousarray(queries, dtype=np.float32)
        num_q, dim = queries.shape
        errmsg = ctypes.c_char_p()
        res = _lib.gpu_ivf_flat_search_float(self.handle, queries.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), num_q, dim, k, search_params, ctypes.byref(errmsg))
        _check_error(errmsg)
        neighbors = np.zeros((num_q, k), dtype=np.int64)
        distances = np.zeros((num_q, k), dtype=np.float32)
        _lib.gpu_ivf_flat_get_neighbors(res.result_ptr, num_q * k, neighbors.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)))
        _lib.gpu_ivf_flat_get_distances(res.result_ptr, num_q * k, distances.ctypes.data_as(ctypes.POINTER(ctypes.c_float)))
        _lib.gpu_ivf_flat_free_result(res.result_ptr); return neighbors, distances

    def __len__(self): return _lib.gpu_ivf_flat_len(self.handle)
    def capacity(self): return _lib.gpu_ivf_flat_cap(self.handle)

    def __del__(self):
        if hasattr(self, 'handle') and self.handle:
            errmsg = ctypes.c_char_p(); _lib.gpu_ivf_flat_destroy(self.handle, ctypes.byref(errmsg))

class IvfPqIndex:
    def __init__(self, handle): self.handle = handle

    @classmethod
    def create(cls, dataset, metric=DistanceType.L2Expanded, build_params=None, devices=[0], nthread=4, dist_mode=DistributionMode.SINGLE_GPU, qtype=Quantization.F32, ids=None):
        if build_params is None: build_params = IvfPqBuildParams.default()
        dataset = np.ascontiguousarray(dataset, dtype=np.float32)
        count, dim = dataset.shape
        dev_arr = (ctypes.c_int * len(devices))(*devices)
        id_ptr = ids.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)) if ids is not None else None
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_ivf_pq_new(dataset.ctypes.data_as(ctypes.c_void_p), count, dim, int(metric), build_params, dev_arr, len(devices), nthread, int(dist_mode), int(qtype), id_ptr, ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    @classmethod
    def create_empty(cls, total_count, dimension, metric=DistanceType.L2Expanded, build_params=None, devices=[0], nthread=4, dist_mode=DistributionMode.SINGLE_GPU, qtype=Quantization.F32, ids=None):
        if build_params is None: build_params = IvfPqBuildParams.default()
        dev_arr = (ctypes.c_int * len(devices))(*devices)
        id_ptr = ids.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)) if ids is not None else None
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_ivf_pq_new_empty(total_count, dimension, int(metric), build_params, dev_arr, len(devices), nthread, int(dist_mode), int(qtype), id_ptr, ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    def start(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_start(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def build(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_build(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def add_chunk(self, chunk):
        chunk = np.ascontiguousarray(chunk, dtype=np.float32)
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_add_chunk_float(self.handle, chunk.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(chunk), ctypes.byref(errmsg)); _check_error(errmsg)
    def delete_id(self, id_val):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_delete_id(self.handle, id_val, ctypes.byref(errmsg)); _check_error(errmsg)
    def save(self, filename):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_save(self.handle, filename.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)
    def save_dir(self, directory):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_save_dir(self.handle, directory.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)
    def load_dir(self, directory):
        errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_load_dir(self.handle, directory.encode('utf-8'), ctypes.byref(errmsg)); _check_error(errmsg)

    def search(self, queries, k, search_params=None):
        if search_params is None: search_params = IvfPqSearchParams.default()
        queries = np.ascontiguousarray(queries, dtype=np.float32)
        num_q, dim = queries.shape
        errmsg = ctypes.c_char_p()
        res = _lib.gpu_ivf_pq_search_float(self.handle, queries.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), num_q, dim, k, search_params, ctypes.byref(errmsg))
        _check_error(errmsg)
        neighbors = np.zeros((num_q, k), dtype=np.int64)
        distances = np.zeros((num_q, k), dtype=np.float32)
        _lib.gpu_ivf_pq_get_neighbors(res.result_ptr, num_q * k, neighbors.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)))
        _lib.gpu_ivf_pq_get_distances(res.result_ptr, num_q * k, distances.ctypes.data_as(ctypes.POINTER(ctypes.c_float)))
        _lib.gpu_ivf_pq_free_result(res.result_ptr); return neighbors, distances

    def __len__(self): return _lib.gpu_ivf_pq_len(self.handle)
    def capacity(self): return _lib.gpu_ivf_pq_cap(self.handle)
    def info(self):
        errmsg = ctypes.c_char_p(); s = _lib.gpu_ivf_pq_info(self.handle, ctypes.byref(errmsg))
        _check_error(errmsg); return s.decode('utf-8') if s else ""

    def __del__(self):
        if hasattr(self, 'handle') and self.handle:
            errmsg = ctypes.c_char_p(); _lib.gpu_ivf_pq_destroy(self.handle, ctypes.byref(errmsg))

class BruteForceIndex:
    def __init__(self, handle): self.handle = handle

    @classmethod
    def create(cls, dataset, metric=DistanceType.L2Expanded, nthread=4, device_id=0, qtype=Quantization.F32):
        dataset = np.ascontiguousarray(dataset, dtype=np.float32)
        count, dim = dataset.shape
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_brute_force_new(dataset.ctypes.data_as(ctypes.c_void_p), count, dim, int(metric), nthread, device_id, int(qtype), ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    @classmethod
    def create_empty(cls, total_count, dimension, metric=DistanceType.L2Expanded, nthread=4, device_id=0, qtype=Quantization.F32):
        errmsg = ctypes.c_char_p()
        h = _lib.gpu_brute_force_new_empty(total_count, dimension, int(metric), nthread, device_id, int(qtype), ctypes.byref(errmsg))
        _check_error(errmsg); return cls(h)

    def start(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_brute_force_start(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def build(self):
        errmsg = ctypes.c_char_p(); _lib.gpu_brute_force_build(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
    def add_chunk(self, chunk):
        chunk = np.ascontiguousarray(chunk, dtype=np.float32)
        errmsg = ctypes.c_char_p(); _lib.gpu_brute_force_add_chunk_float(self.handle, chunk.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(chunk), ctypes.byref(errmsg)); _check_error(errmsg)

    def search(self, queries, k):
        queries = np.ascontiguousarray(queries, dtype=np.float32)
        num_q, dim = queries.shape
        errmsg = ctypes.c_char_p()
        res_ptr = _lib.gpu_brute_force_search_float(self.handle, queries.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), num_q, dim, k, ctypes.byref(errmsg))
        _check_error(errmsg)
        neighbors = np.zeros((num_q, k), dtype=np.int64)
        distances = np.zeros((num_q, k), dtype=np.float32)
        _lib.gpu_brute_force_get_results(res_ptr, num_q, k, neighbors.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)), distances.ctypes.data_as(ctypes.POINTER(ctypes.c_float)))
        _lib.gpu_brute_force_free_search_result(res_ptr); return neighbors, distances

    def __len__(self): return _lib.gpu_brute_force_len(self.handle)
    def capacity(self): return _lib.gpu_brute_force_cap(self.handle)
    def info(self):
        errmsg = ctypes.c_char_p(); s = _lib.gpu_brute_force_info(self.handle, ctypes.byref(errmsg))
        _check_error(errmsg); return s.decode('utf-8') if s else ""

    def __del__(self):
        if hasattr(self, 'handle') and self.handle:
            errmsg = ctypes.c_char_p(); _lib.gpu_brute_force_destroy(self.handle, ctypes.byref(errmsg))

class KMeans:
    def __init__(self, n_clusters, dimension, metric=DistanceType.L2Expanded, max_iter=300, device_id=0, nthread=4, qtype=Quantization.F32):
        errmsg = ctypes.c_char_p()
        self.handle = _lib.gpu_kmeans_new(n_clusters, dimension, int(metric), max_iter, device_id, nthread, int(qtype), ctypes.byref(errmsg))
        _check_error(errmsg)
        _lib.gpu_kmeans_start(self.handle, ctypes.byref(errmsg)); _check_error(errmsg)
        self.n_clusters, self.dimension = n_clusters, dimension

    def fit(self, X):
        X = np.ascontiguousarray(X, dtype=np.float32)
        errmsg = ctypes.c_char_p()
        _lib.gpu_kmeans_fit(self.handle, X.ctypes.data_as(ctypes.c_void_p), len(X), ctypes.byref(errmsg))
        _check_error(errmsg)

    def fit_predict(self, X):
        X = np.ascontiguousarray(X, dtype=np.float32)
        errmsg = ctypes.c_char_p()
        res = _lib.gpu_kmeans_fit_predict_float(self.handle, X.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(X), ctypes.byref(errmsg))
        _check_error(errmsg)
        labels = np.zeros(len(X), dtype=np.int64)
        _lib.gpu_kmeans_get_labels(res.result_ptr, len(X), labels.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)))
        _lib.gpu_kmeans_free_result(res.result_ptr); return labels, res.inertia, res.n_iter

    def predict(self, X):
        X = np.ascontiguousarray(X, dtype=np.float32)
        errmsg = ctypes.c_char_p()
        res = _lib.gpu_kmeans_predict(self.handle, X.ctypes.data_as(ctypes.c_void_p), len(X), ctypes.byref(errmsg))
        _check_error(errmsg)
        labels = np.zeros(len(X), dtype=np.int64)
        _lib.gpu_kmeans_get_labels(res.result_ptr, len(X), labels.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)))
        _lib.gpu_kmeans_free_result(res.result_ptr); return labels, res.inertia

    def predict_float(self, X):
        X = np.ascontiguousarray(X, dtype=np.float32)
        errmsg = ctypes.c_char_p()
        res = _lib.gpu_kmeans_predict_float(self.handle, X.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(X), ctypes.byref(errmsg))
        _check_error(errmsg)
        labels = np.zeros(len(X), dtype=np.int64)
        _lib.gpu_kmeans_get_labels(res.result_ptr, len(X), labels.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)))
        _lib.gpu_kmeans_free_result(res.result_ptr); return labels, res.inertia

    def get_centroids(self):
        centroids = np.zeros((self.n_clusters, self.dimension), dtype=np.float32)
        errmsg = ctypes.c_char_p(); _lib.gpu_kmeans_get_centroids(self.handle, centroids.ctypes.data_as(ctypes.c_void_p), ctypes.byref(errmsg))
        _check_error(errmsg); return centroids

    def train_quantizer(self, train_data):
        train_data = np.ascontiguousarray(train_data, dtype=np.float32)
        errmsg = ctypes.c_char_p(); _lib.gpu_kmeans_train_quantizer(self.handle, train_data.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), len(train_data), ctypes.byref(errmsg)); _check_error(errmsg)

    def info(self):
        errmsg = ctypes.c_char_p(); s = _lib.gpu_kmeans_info(self.handle, ctypes.byref(errmsg))
        _check_error(errmsg); return s.decode('utf-8') if s else ""

    def __del__(self):
        if hasattr(self, 'handle') and self.handle:
            errmsg = ctypes.c_char_p(); _lib.gpu_kmeans_destroy(self.handle, ctypes.byref(errmsg))

# --- Global Utility Functions ---

def get_devices():
    count = _lib.gpu_get_device_count() if _lib else 0
    if count > 0:
        devs = (ctypes.c_int * count)(); _lib.gpu_get_device_list(devs, count)
        return [devs[i] for i in range(count)]
    return []

def pairwise_distance(x, y, metric=DistanceType.L2Expanded, qtype=Quantization.F32):
    x, y = np.ascontiguousarray(x, dtype=np.float32), np.ascontiguousarray(y, dtype=np.float32)
    n_x, n_y, dim = len(x), len(y), x.shape[1]
    dist = np.zeros((n_x, n_y), dtype=np.float32); errmsg = ctypes.c_char_p()
    _lib.gpu_pairwise_distance(x.ctypes.data_as(ctypes.c_void_p), n_x, y.ctypes.data_as(ctypes.c_void_p), n_y, dim, int(metric), int(qtype), dist.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), ctypes.byref(errmsg))
    _check_error(errmsg); return dist

def adhoc_brute_force_search(dataset, queries, k, metric=DistanceType.L2Expanded):
    dataset, queries = np.ascontiguousarray(dataset, dtype=np.float32), np.ascontiguousarray(queries, dtype=np.float32)
    n_rows, n_q, dim = len(dataset), len(queries), dataset.shape[1]
    neighbors, distances = np.zeros((n_q, k), dtype=np.int64), np.zeros((n_q, k), dtype=np.float32)
    errmsg = ctypes.c_char_p()
    _lib.gpu_adhoc_brute_force_search_float(dataset.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), n_rows, dim, queries.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), n_q, k, int(metric), neighbors.ctypes.data_as(ctypes.POINTER(ctypes.c_int64)), distances.ctypes.data_as(ctypes.POINTER(ctypes.c_float)), ctypes.byref(errmsg))
    _check_error(errmsg); return neighbors, distances
