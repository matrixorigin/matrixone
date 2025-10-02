# Copyright 2021 - 2022 Matrix Origin
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

"""
Offline tests for VectorType SQLAlchemy integration.
"""

import pytest
import sys
from unittest.mock import Mock
from matrixone.sqlalchemy_ext import VectorType, Vectorf32, Vectorf64, VectorTypeDecorator

pytestmark = pytest.mark.vector

# No longer needed - global mocks have been fixed


class TestVectorType:
    """Test VectorType functionality."""

    def test_vector_type_creation(self):
        """Test VectorType creation with different parameters."""
        # Test with dimension
        vec_type = VectorType(dimension=128, precision="f32")
        assert vec_type.dimension == 128
        assert vec_type.precision == "f32"

        # Test without dimension
        vec_type_no_dim = VectorType(precision="f64")
        assert vec_type_no_dim.dimension is None
        assert vec_type_no_dim.precision == "f64"

    def test_vector_type_column_spec(self):
        """Test VectorType column specification."""
        # Test with dimension
        vec_type = VectorType(dimension=64, precision="f32")
        spec = vec_type.get_col_spec()
        assert spec == "vecf32(64)"

        # Test without dimension
        vec_type_no_dim = VectorType(precision="f64")
        spec = vec_type_no_dim.get_col_spec()
        assert spec == "vecf64"

    def test_vector_type_bind_processor(self):
        """Test VectorType bind processor."""
        vec_type = VectorType(dimension=3, precision="f32")
        processor = vec_type.bind_processor(None)

        # Test with list
        result = processor([1.0, 2.0, 3.0])
        assert result == "[1.0,2.0,3.0]"

        # Test with string
        result = processor("[1.0,2.0,3.0]")
        assert result == "[1.0,2.0,3.0]"

        # Test with None
        result = processor(None)
        assert result is None

    def test_vector_type_result_processor(self):
        """Test VectorType result processor."""
        vec_type = VectorType(dimension=3, precision="f32")
        processor = vec_type.result_processor(None, None)

        # Test with vector string
        result = processor("[1.0,2.0,3.0]")
        assert result == [1.0, 2.0, 3.0]

        # Test with empty vector
        result = processor("[]")
        assert result == []

        # Test with None
        result = processor(None)
        assert result is None

    def test_vectorf32_convenience_class(self):
        """Test Vectorf32 convenience class."""
        vec32 = Vectorf32(dimension=64)
        assert vec32.dimension == 64
        assert vec32.precision == "f32"

        spec = vec32.get_col_spec()
        assert spec == "vecf32(64)"

    def test_vectorf64_convenience_class(self):
        """Test Vectorf64 convenience class."""
        vec64 = Vectorf64(dimension=128)
        assert vec64.dimension == 128
        assert vec64.precision == "f64"

        spec = vec64.get_col_spec()
        assert spec == "vecf64(128)"

    def test_vector_type_decorator(self):
        """Test VectorTypeDecorator functionality."""
        decorator = VectorTypeDecorator(dimension=256, precision="f32")
        assert decorator.dimension == 256
        assert decorator.precision == "f32"

    def test_vector_type_decorator_bind_processing(self):
        """Test VectorTypeDecorator bind parameter processing."""
        decorator = VectorTypeDecorator(dimension=3, precision="f32")

        # Test with list
        result = decorator.process_bind_param([1.0, 2.0, 3.0], None)
        assert result == "[1.0,2.0,3.0]"

        # Test with string
        result = decorator.process_bind_param("[1.0,2.0,3.0]", None)
        assert result == "[1.0,2.0,3.0]"

        # Test with None
        result = decorator.process_bind_param(None, None)
        assert result is None

    def test_vector_type_decorator_result_processing(self):
        """Test VectorTypeDecorator result processing."""
        decorator = VectorTypeDecorator(dimension=3, precision="f32")

        # Test with vector string
        result = decorator.process_result_value("[1.0,2.0,3.0]", None)
        assert result == [1.0, 2.0, 3.0]

        # Test with empty vector
        result = decorator.process_result_value("[]", None)
        assert result == []

        # Test with None
        result = decorator.process_result_value(None, None)
        assert result is None

    def test_vector_type_repr(self):
        """Test VectorType string representation."""
        # Test with dimension
        vec_type = VectorType(dimension=128, precision="f32")
        repr_str = repr(vec_type)
        assert "VectorType" in repr_str
        assert "dimension=128" in repr_str
        assert "precision='f32'" in repr_str

        # Test without dimension
        vec_type_no_dim = VectorType(precision="f64")
        repr_str = repr(vec_type_no_dim)
        assert "VectorType" in repr_str
        assert "precision='f64'" in repr_str

    def test_vector_type_decorator_repr(self):
        """Test VectorTypeDecorator string representation."""
        # Test with dimension
        decorator = VectorTypeDecorator(dimension=256, precision="f32")
        repr_str = repr(decorator)
        assert "VectorTypeDecorator" in repr_str
        assert "dimension=256" in repr_str
        assert "precision='f32'" in repr_str

        # Test without dimension
        decorator_no_dim = VectorTypeDecorator(precision="f64")
        repr_str = repr(decorator_no_dim)
        assert "VectorTypeDecorator" in repr_str
        assert "precision='f64'" in repr_str
