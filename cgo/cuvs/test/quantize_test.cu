/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "quantize.hpp"
#include "test_framework.hpp"
#include <raft/core/resources.hpp>
#include <cstdio>
#include <vector>
#include <cmath>
#include <sstream>

using namespace matrixone;

TEST(UtilsTest, ScalarQuantizerLifecycle) {
    raft::resources res;
    const int64_t count = 100;
    const int64_t dimension = 8;
    
    // 1. Train
    scalar_quantizer_t<float> quantizer;
    ASSERT_FALSE(quantizer.is_trained());
    
    auto matrix = raft::make_device_matrix<float, int64_t>(res, count, dimension);
    std::vector<float> host_data(count * dimension);
    for (size_t i = 0; i < host_data.size(); ++i) {
        host_data[i] = static_cast<float>(i % 100) / 50.0f - 1.0f; // range [-1, 0.98]
    }
    raft::copy(matrix.data_handle(), host_data.data(), host_data.size(), raft::resource::get_cuda_stream(res));
    raft::resource::sync_stream(res);
    
    quantizer.train(res, matrix.view());
    ASSERT_TRUE(quantizer.is_trained());
    
    // 2. Getters
    float q_min = quantizer.min();
    float q_max = quantizer.max();
    // Default quantile is 1.0, so it should be exactly -1.0 and 0.98
    ASSERT_TRUE(std::abs(q_min - (-1.0f)) < 1e-5f);
    ASSERT_TRUE(std::abs(q_max - 0.98f) < 1e-5f);
    
    // 3. Constructor
    scalar_quantizer_t<float> quantizer2(q_min, q_max);
    ASSERT_TRUE(quantizer2.is_trained());
    ASSERT_EQ(quantizer2.min(), q_min);
    ASSERT_EQ(quantizer2.max(), q_max);
    
    // 4. Save/Load
    const std::string filename = "test_quantizer.bin";
    quantizer.save_to_file(filename);
    
    scalar_quantizer_t<float> quantizer3;
    quantizer3.load_from_file(filename);
    ASSERT_TRUE(quantizer3.is_trained());
    ASSERT_EQ(quantizer3.min(), q_min);
    ASSERT_EQ(quantizer3.max(), q_max);
    std::remove(filename.c_str());
    
    // 5. Serialize/Deserialize
    std::stringstream ss;
    quantizer.serialize(ss);
    
    scalar_quantizer_t<float> quantizer4;
    quantizer4.deserialize(ss);
    ASSERT_TRUE(quantizer4.is_trained());
    ASSERT_EQ(quantizer4.min(), q_min);
    ASSERT_EQ(quantizer4.max(), q_max);

    // 6. SetQuantizer
    scalar_quantizer_t<float> quantizer5;
    quantizer5.set_quantizer(0.1f, 0.9f);
    ASSERT_TRUE(quantizer5.is_trained());
    ASSERT_EQ(quantizer5.min(), 0.1f);
    ASSERT_EQ(quantizer5.max(), 0.9f);

    // 7. Getters again
    ASSERT_EQ(quantizer5.min(), 0.1f);
    ASSERT_EQ(quantizer5.max(), 0.9f);
    
    // 8. Transform
    std::vector<int8_t> result_host(count * dimension);
    quantizer.transform(res, matrix.view(), result_host.data(), false);
    
    bool non_zero = false;
    for (auto v : result_host) if (v != 0) non_zero = true;
    ASSERT_TRUE(non_zero);
}
