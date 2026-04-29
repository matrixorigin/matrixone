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

if __name__ == '__main__':
    unittest.main()
