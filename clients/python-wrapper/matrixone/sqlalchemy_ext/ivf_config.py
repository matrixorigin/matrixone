"""
IVF (Inverted File) index configuration utilities for MatrixOne.
"""

from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine


class IVFConfig:
    """
    Configuration manager for IVF indexing in MatrixOne.

    This class provides utilities to enable/disable IVF indexing
    and configure search parameters.
    """

    def __init__(self, engine: Engine):
        """
        Initialize IVF configuration manager.

        Args:
            engine: SQLAlchemy engine connected to MatrixOne
        """
        self.engine = engine

    def enable_ivf_indexing(self) -> bool:
        """
        Enable IVF indexing in MatrixOne.

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text("SET experimental_ivf_index = 1"))
            return True
        except Exception:
            return False

    def disable_ivf_indexing(self) -> bool:
        """
        Disable IVF indexing in MatrixOne.

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text("SET experimental_ivf_index = 0"))
            return True
        except Exception:
            return False

    def set_probe_limit(self, limit: int) -> bool:
        """
        Set the probe limit for IVF index search.

        Args:
            limit: Number of probes to use during search

        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"SET probe_limit = {limit}"))
            return True
        except Exception:
            return False

    def get_ivf_status(self) -> Dict[str, Any]:
        """
        Get current IVF configuration status.

        Returns:
            Dictionary with current IVF settings
        """
        status = {"ivf_enabled": None, "probe_limit": None, "error": None}

        try:
            with self.engine.begin() as conn:
                # Get IVF index setting
                result = conn.execute(text("SHOW VARIABLES LIKE 'experimental_ivf_index'"))
                ivf_setting = result.fetchone()
                if ivf_setting:
                    status["ivf_enabled"] = ivf_setting[1] == "1"

                # Get probe limit setting
                result = conn.execute(text("SHOW VARIABLES LIKE 'probe_limit'"))
                probe_setting = result.fetchone()
                if probe_setting:
                    status["probe_limit"] = int(probe_setting[1])

        except Exception as e:
            status["error"] = str(e)

        return status

    def configure_ivf(self, enabled: bool = True, probe_limit: Optional[int] = None) -> bool:
        """
        Configure IVF indexing with multiple parameters.

        Args:
            enabled: Whether to enable IVF indexing
            probe_limit: Probe limit for search (optional)

        Returns:
            True if all operations successful, False otherwise
        """
        success = True

        # Enable/disable IVF
        if enabled:
            success &= self.enable_ivf_indexing()
        else:
            success &= self.disable_ivf_indexing()

        # Set probe limit if specified
        if probe_limit is not None:
            success &= self.set_probe_limit(probe_limit)

        return success

    def is_ivf_supported(self) -> bool:
        """
        Check if IVF indexing is supported in the current MatrixOne instance.

        Returns:
            True if IVF is supported, False otherwise
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text("SHOW VARIABLES LIKE 'experimental_ivf_index'"))
            return True
        except Exception:
            return False


def create_ivf_config(engine: Engine) -> IVFConfig:
    """
    Create an IVF configuration manager.

    Args:
        engine: SQLAlchemy engine connected to MatrixOne

    Returns:
        IVFConfig instance
    """
    return IVFConfig(engine)


# Convenience functions for direct use
def enable_ivf_indexing(engine: Engine) -> bool:
    """
    Enable IVF indexing.

    Args:
        engine: SQLAlchemy engine

    Returns:
        True if successful, False otherwise
    """
    config = IVFConfig(engine)
    return config.enable_ivf_indexing()


def disable_ivf_indexing(engine: Engine) -> bool:
    """
    Disable IVF indexing.

    Args:
        engine: SQLAlchemy engine

    Returns:
        True if successful, False otherwise
    """
    config = IVFConfig(engine)
    return config.disable_ivf_indexing()


def set_probe_limit(engine: Engine, limit: int) -> bool:
    """
    Set probe limit for IVF search.

    Args:
        engine: SQLAlchemy engine
        limit: Probe limit value

    Returns:
        True if successful, False otherwise
    """
    config = IVFConfig(engine)
    return config.set_probe_limit(limit)


def get_ivf_status(engine: Engine) -> Dict[str, Any]:
    """
    Get current IVF configuration status.

    Args:
        engine: SQLAlchemy engine

    Returns:
        Dictionary with current IVF settings
    """
    config = IVFConfig(engine)
    return config.get_ivf_status()
