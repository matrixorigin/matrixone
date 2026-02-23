"""
Unit tests for IVF LIMIT BY RANK feature.

Tests cover:
- IVFRankMode enum and IVFRankOptions class
- Parameter validation
- SQL generation
- Mode conversions
"""

import pytest
from matrixone.ivf_rank import IVFRankMode, IVFRankOptions


class TestIVFRankMode:
    """Test IVFRankMode enumeration."""

    def test_ivf_rank_mode_values(self):
        """Test that IVFRankMode has correct values."""
        assert IVFRankMode.PRE.value == "pre"
        assert IVFRankMode.POST.value == "post"
        assert IVFRankMode.FORCE.value == "force"

    def test_ivf_rank_mode_members(self):
        """Test that all expected modes exist."""
        modes = {mode.value for mode in IVFRankMode}
        assert modes == {"pre", "post", "force"}


class TestIVFRankOptions:
    """Test IVFRankOptions class."""

    def test_default_mode(self):
        """Test that default mode is POST."""
        options = IVFRankOptions()
        assert options.mode == IVFRankMode.POST

    def test_init_with_enum(self):
        """Test initialization with IVFRankMode enum."""
        options = IVFRankOptions(mode=IVFRankMode.PRE)
        assert options.mode == IVFRankMode.PRE

        options = IVFRankOptions(mode=IVFRankMode.FORCE)
        assert options.mode == IVFRankMode.FORCE

    def test_init_with_string(self):
        """Test initialization with string mode."""
        options = IVFRankOptions(mode="pre")
        assert options.mode == IVFRankMode.PRE

        options = IVFRankOptions(mode="post")
        assert options.mode == IVFRankMode.POST

        options = IVFRankOptions(mode="force")
        assert options.mode == IVFRankMode.FORCE

    def test_init_with_uppercase_string(self):
        """Test initialization with uppercase string mode."""
        options = IVFRankOptions(mode="PRE")
        assert options.mode == IVFRankMode.PRE

        options = IVFRankOptions(mode="POST")
        assert options.mode == IVFRankMode.POST

    def test_init_with_invalid_string(self):
        """Test that invalid string raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            IVFRankOptions(mode="invalid")
        assert "Invalid IVF rank mode" in str(exc_info.value)

    def test_init_with_invalid_type(self):
        """Test that invalid type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            IVFRankOptions(mode=123)
        assert "mode must be IVFRankMode or string" in str(exc_info.value)

    def test_to_sql_option(self):
        """Test SQL option string generation."""
        options = IVFRankOptions(mode=IVFRankMode.PRE)
        assert options.to_sql_option() == "mode=pre"

        options = IVFRankOptions(mode=IVFRankMode.POST)
        assert options.to_sql_option() == "mode=post"

        options = IVFRankOptions(mode=IVFRankMode.FORCE)
        assert options.to_sql_option() == "mode=force"

    def test_to_dict(self):
        """Test dictionary conversion."""
        options = IVFRankOptions(mode=IVFRankMode.PRE)
        assert options.to_dict() == {"mode": "pre"}

        options = IVFRankOptions(mode=IVFRankMode.POST)
        assert options.to_dict() == {"mode": "post"}

    def test_repr(self):
        """Test string representation."""
        options = IVFRankOptions(mode=IVFRankMode.PRE)
        assert repr(options) == "IVFRankOptions(mode=pre)"

        options = IVFRankOptions(mode=IVFRankMode.FORCE)
        assert repr(options) == "IVFRankOptions(mode=force)"

    def test_equality(self):
        """Test equality comparison."""
        options1 = IVFRankOptions(mode=IVFRankMode.PRE)
        options2 = IVFRankOptions(mode=IVFRankMode.PRE)
        options3 = IVFRankOptions(mode=IVFRankMode.POST)

        assert options1 == options2
        assert options1 != options3

    def test_equality_with_non_ivf_rank_options(self):
        """Test equality with non-IVFRankOptions object."""
        options = IVFRankOptions(mode=IVFRankMode.PRE)
        assert options != "pre"
        assert options != 123
        assert options != None


class TestIVFRankOptionsIntegration:
    """Integration tests for IVFRankOptions."""

    def test_all_modes_supported(self):
        """Test that all modes can be created and converted."""
        for mode in IVFRankMode:
            options = IVFRankOptions(mode=mode)
            assert options.mode == mode
            assert options.to_sql_option() == f"mode={mode.value}"
            assert options.to_dict() == {"mode": mode.value}

    def test_string_mode_case_insensitive(self):
        """Test that string mode is case-insensitive."""
        for mode in IVFRankMode:
            options_lower = IVFRankOptions(mode=mode.value.lower())
            options_upper = IVFRankOptions(mode=mode.value.upper())
            options_mixed = IVFRankOptions(mode=mode.value.capitalize())

            assert options_lower.mode == mode
            assert options_upper.mode == mode
            assert options_mixed.mode == mode


class TestVectorManagerSearchWithRank:
    """Test VectorManager.search_with_rank() method."""

    def test_search_with_rank_method_exists(self):
        """Test that search_with_rank method exists."""
        from matrixone.vector_manager import VectorManager

        assert hasattr(VectorManager, 'search_with_rank')

    def test_async_search_with_rank_method_exists(self):
        """Test that async search_with_rank method exists."""
        from matrixone.vector_manager import AsyncVectorManager

        assert hasattr(AsyncVectorManager, 'search_with_rank')

    def test_search_with_rank_signature(self):
        """Test search_with_rank method signature."""
        from matrixone.vector_manager import VectorManager
        import inspect

        sig = inspect.signature(VectorManager.search_with_rank)
        params = list(sig.parameters.keys())

        # Check required parameters
        assert 'table_name' in params
        assert 'vector_column' in params
        assert 'query_vector' in params

        # Check optional parameters
        assert 'limit' in params
        assert 'select_columns' in params
        assert 'where_clause' in params
        assert 'distance_type' in params
        assert 'rank_mode' in params

    def test_rank_mode_parameter_accepts_enum(self):
        """Test that rank_mode parameter accepts IVFRankMode enum."""
        # This is a type hint test - verify the annotation
        from matrixone.vector_manager import VectorManager
        import inspect

        sig = inspect.signature(VectorManager.search_with_rank)
        rank_mode_param = sig.parameters['rank_mode']

        # Check annotation contains IVFRankMode
        annotation_str = str(rank_mode_param.annotation)
        assert 'IVFRankMode' in annotation_str or 'str' in annotation_str

    def test_rank_mode_parameter_accepts_string(self):
        """Test that rank_mode parameter accepts string."""
        from matrixone.vector_manager import VectorManager
        import inspect

        sig = inspect.signature(VectorManager.search_with_rank)
        rank_mode_param = sig.parameters['rank_mode']

        # Check annotation contains str
        annotation_str = str(rank_mode_param.annotation)
        assert 'str' in annotation_str or 'Union' in annotation_str

    def test_distance_type_parameter_default(self):
        """Test that distance_type has default value."""
        from matrixone.vector_manager import VectorManager
        import inspect

        sig = inspect.signature(VectorManager.search_with_rank)
        distance_type_param = sig.parameters['distance_type']

        assert distance_type_param.default == "l2"

    def test_limit_parameter_default(self):
        """Test that limit has default value."""
        from matrixone.vector_manager import VectorManager
        import inspect

        sig = inspect.signature(VectorManager.search_with_rank)
        limit_param = sig.parameters['limit']

        assert limit_param.default == 10

    def test_rank_mode_parameter_default_is_none(self):
        """Test that rank_mode defaults to None."""
        from matrixone.vector_manager import VectorManager
        import inspect

        sig = inspect.signature(VectorManager.search_with_rank)
        rank_mode_param = sig.parameters['rank_mode']

        assert rank_mode_param.default is None
