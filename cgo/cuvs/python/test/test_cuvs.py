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

import unittest
import numpy as np
import os
import sys
import tempfile

# Add the parent directory to sys.path so we can import cuvs
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import cuvs

class TestCuvs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Skip tests if library is not loaded
        if cuvs._lib is None:
            raise unittest.SkipTest("libmocuvs.so not found")
        
        # Check if GPU is available
        if len(cuvs.get_devices()) == 0:
            raise unittest.SkipTest("No GPU devices found")

    def setUp(self):
        # Create a small random dataset
        self.n_rows = 1000
        self.dim = 64
        self.k = 10
        self.dataset = np.random.random((self.n_rows, self.dim)).astype(np.float32)
        self.queries = np.random.random((5, self.dim)).astype(np.float32)

    def test_brute_force(self):
        index = cuvs.BruteForceIndex.create(self.dataset)
        index.start()
        index.build()
        neighbors, distances = index.search(self.queries, self.k)
        
        self.assertEqual(neighbors.shape, (5, self.k))
        self.assertEqual(distances.shape, (5, self.k))
        self.assertTrue(np.all(neighbors >= 0))
        self.assertTrue(np.all(neighbors < self.n_rows))

    def test_cagra(self):
        index = cuvs.CagraIndex.create(self.dataset)
        index.start()
        index.build()
        neighbors, distances = index.search(self.queries, self.k)
        
        self.assertEqual(neighbors.shape, (5, self.k))
        self.assertEqual(distances.shape, (5, self.k))
        self.assertTrue(np.all(neighbors >= 0))
        self.assertTrue(np.all(neighbors < self.n_rows))

    def test_ivf_flat(self):
        build_params = cuvs.IvfFlatBuildParams(n_lists=32, add_data_on_build=True, kmeans_trainset_fraction=1.0)
        index = cuvs.IvfFlatIndex.create(self.dataset, build_params=build_params)
        index.start()
        index.build()
        neighbors, distances = index.search(self.queries, self.k)
        
        self.assertEqual(neighbors.shape, (5, self.k))
        self.assertEqual(distances.shape, (5, self.k))

    def test_ivf_pq(self):
        build_params = cuvs.IvfPqBuildParams(n_lists=32, m=8, bits_per_code=8, add_data_on_build=True, kmeans_trainset_fraction=1.0)
        index = cuvs.IvfPqIndex.create(self.dataset, build_params=build_params)
        index.start()
        index.build()
        neighbors, distances = index.search(self.queries, self.k)
        
        self.assertEqual(neighbors.shape, (5, self.k))
        self.assertEqual(distances.shape, (5, self.k))

    def test_kmeans(self):
        n_clusters = 5
        kmeans = cuvs.KMeans(n_clusters=n_clusters, dimension=self.dim)
        
        # KMeans requires training (fit) before predict
        # but in our current C wrapper, gpu_kmeans_fit might expect data of type qtype
        labels, pred_inertia, n_iter = kmeans.fit_predict(self.dataset)
        
        self.assertGreaterEqual(n_iter, 0)
        
        centroids = kmeans.get_centroids()
        self.assertEqual(centroids.shape, (n_clusters, self.dim))
        
        # Now we can predict on queries
        labels, pred_inertia = kmeans.predict(self.queries)
        self.assertEqual(labels.shape, (5,))
        self.assertTrue(np.all(labels >= 0))
        self.assertTrue(np.all(labels < n_clusters))

    def test_pairwise_distance(self):
        dist = cuvs.pairwise_distance(self.queries, self.dataset[:10])
        self.assertEqual(dist.shape, (5, 10))
        self.assertTrue(np.all(dist >= 0))

    def test_adhoc_search(self):
        neighbors, distances = cuvs.adhoc_brute_force_search(self.dataset, self.queries, self.k)
        self.assertEqual(neighbors.shape, (5, self.k))
        self.assertEqual(distances.shape, (5, self.k))

    # ---- Pre-filter (INCLUDE-columns) coverage ----
    #
    # Predicate JSON uses integer column indices (not names): e.g.
    #   [{"col":0,"op":">=","val":0.5}]
    # See cgo/cuvs/filter.hpp parse_preds() / parse_filter_col_meta().

    def _build_filter_index(self, factory, ids, prices, **kwargs):
        # Common create_empty -> set_filter_columns -> add_filter_chunk ->
        # add_chunk -> build flow. set_filter_columns must precede build().
        idx = factory.create_empty(self.n_rows, self.dim, ids=ids, **kwargs)
        idx.start()
        idx.set_filter_columns('[{"name":"price","type":2}]', self.n_rows)
        idx.add_filter_chunk(0, prices, self.n_rows)
        idx.add_chunk(self.dataset, ids=ids)
        idx.build()
        return idx

    def _check_filtered(self, neighbors, distances, prices, threshold):
        self.assertEqual(neighbors.shape, (5, self.k))
        self.assertEqual(distances.shape, (5, self.k))
        # Every non-sentinel neighbor must satisfy the predicate (price >= threshold).
        valid = neighbors[neighbors >= 0]
        if valid.size:
            self.assertTrue(np.all(prices[valid] >= threshold))

    def test_cagra_filter(self):
        ids = np.arange(self.n_rows, dtype=np.int64)
        prices = np.random.random(self.n_rows).astype(np.float32)
        idx = self._build_filter_index(cuvs.CagraIndex, ids, prices)
        nbrs, dists = idx.search_with_filter(
            self.queries, self.k, '[{"col":0,"op":">=","val":0.5}]')
        self._check_filtered(nbrs, dists, prices, 0.5)

    def test_ivf_flat_filter(self):
        ids = np.arange(self.n_rows, dtype=np.int64)
        prices = np.random.random(self.n_rows).astype(np.float32)
        bp = cuvs.IvfFlatBuildParams(n_lists=32, add_data_on_build=True, kmeans_trainset_fraction=1.0)
        idx = self._build_filter_index(cuvs.IvfFlatIndex, ids, prices, build_params=bp)
        nbrs, dists = idx.search_with_filter(
            self.queries, self.k, '[{"col":0,"op":">=","val":0.5}]')
        self._check_filtered(nbrs, dists, prices, 0.5)

    def test_ivf_pq_filter(self):
        ids = np.arange(self.n_rows, dtype=np.int64)
        prices = np.random.random(self.n_rows).astype(np.float32)
        bp = cuvs.IvfPqBuildParams(n_lists=32, m=8, bits_per_code=8, add_data_on_build=True, kmeans_trainset_fraction=1.0)
        idx = self._build_filter_index(cuvs.IvfPqIndex, ids, prices, build_params=bp)
        nbrs, dists = idx.search_with_filter(
            self.queries, self.k, '[{"col":0,"op":">=","val":0.5}]')
        self._check_filtered(nbrs, dists, prices, 0.5)

    def test_filter_empty_predicate_matches_unfiltered(self):
        # Empty preds_json must yield unfiltered behavior — sanity-checks the
        # NULL-propagation path through ctypes.
        ids = np.arange(self.n_rows, dtype=np.int64)
        prices = np.random.random(self.n_rows).astype(np.float32)
        bp = cuvs.IvfFlatBuildParams(n_lists=32, add_data_on_build=True, kmeans_trainset_fraction=1.0)
        idx = self._build_filter_index(cuvs.IvfFlatIndex, ids, prices, build_params=bp)
        nbrs, _ = idx.search_with_filter(self.queries, self.k, None)
        self.assertEqual(nbrs.shape, (5, self.k))
        self.assertTrue(np.all(nbrs >= 0))

    # ---- load_dir with target_mode ----

    def test_ivf_flat_save_load_dir(self):
        bp = cuvs.IvfFlatBuildParams(n_lists=32, add_data_on_build=True, kmeans_trainset_fraction=1.0)
        idx = cuvs.IvfFlatIndex.create(self.dataset, build_params=bp)
        idx.start()
        idx.build()
        original_len = len(idx)

        with tempfile.TemporaryDirectory() as d:
            idx.save_dir(d)
            loaded = cuvs.IvfFlatIndex.create_empty(self.n_rows, self.dim, build_params=bp)
            loaded.start()
            loaded.load_dir(d, target_mode=cuvs.DistributionMode.SINGLE_GPU)
            self.assertEqual(len(loaded), original_len)
            nbrs, _ = loaded.search(self.queries, self.k)
            self.assertEqual(nbrs.shape, (5, self.k))

if __name__ == '__main__':
    unittest.main()
