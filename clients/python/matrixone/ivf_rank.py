"""
IVF Vector Search with LIMIT BY RANK support.

This module provides support for MatrixOne's IVF (Inverted File) index with
LIMIT BY RANK feature, allowing fine-grained control over vector search
execution strategies.

Supported modes:
- pre: Pre-ranking mode - applies ranking before IVF filtering
- post: Post-ranking mode - applies ranking after IVF filtering
- force: Force mode - forces IVF index usage regardless of optimizer decision
"""

from enum import Enum
from typing import Optional, Dict, Any


class IVFRankMode(Enum):
    """
    Enumeration of IVF ranking modes for vector search optimization.

    Attributes:
        PRE: Pre-filter mode. Applies WHERE clause filtering BEFORE vector search.
             Requires high selectivity of the filter condition for good performance.
             If filter selectivity is poor, performance degrades significantly.
             Best for: Cases with highly selective filters (e.g., filtering to <1% of data).

        POST: Post-filter mode (recommended). Performs vector search FIRST using IVF index,
              then applies WHERE clause filtering on results. Best performance in most cases.
              Recall is typically not affected. Use this as default unless you have
              specific reasons to use PRE mode.
              Best for: Most production scenarios, optimal performance.

        FORCE: Force mode. Explicitly enforces IVF index usage regardless of
               query optimizer's decision. Best for: Debugging, predictable index usage.
    """

    PRE = "pre"
    POST = "post"
    FORCE = "force"


class IVFRankOptions:
    """
    Configuration options for IVF vector search with LIMIT BY RANK.

    This class encapsulates all parameters needed to control how vector
    similarity search is executed when using IVF indexes with ranking.

    Attributes:
        mode (IVFRankMode): The ranking strategy to use. Defaults to POST.

    Example:
        >>> options = IVFRankOptions(mode=IVFRankMode.PRE)
        >>> # Use in vector search query
    """

    def __init__(
        self,
        mode: Optional[IVFRankMode] = None,
    ):
        """
        Initialize IVF rank options.

        Args:
            mode: The ranking mode to use. If None, defaults to POST mode.
                  - PRE: Fast approximate search, may miss some results
                  - POST: Slower but more accurate search
                  - FORCE: Force index usage with strict ranking

        Raises:
            ValueError: If mode is not a valid IVFRankMode or string.
        """
        if mode is None:
            self.mode = IVFRankMode.POST
        elif isinstance(mode, IVFRankMode):
            self.mode = mode
        elif isinstance(mode, str):
            try:
                self.mode = IVFRankMode(mode.lower())
            except ValueError:
                raise ValueError(
                    f"Invalid IVF rank mode: {mode}. " f"Must be one of: {', '.join(m.value for m in IVFRankMode)}"
                )
        else:
            raise ValueError(f"mode must be IVFRankMode or string, got {type(mode).__name__}")

    def to_sql_option(self) -> str:
        """
        Convert options to SQL BY RANK WITH OPTION clause format.

        Returns:
            str: SQL option string suitable for BY RANK WITH OPTION clause.
                 Example: "mode=pre"

        Example:
            >>> options = IVFRankOptions(mode=IVFRankMode.PRE)
            >>> options.to_sql_option()
            'mode=pre'
        """
        return f"mode={self.mode.value}"

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert options to dictionary representation.

        Returns:
            dict: Dictionary with mode as key and mode value as value.

        Example:
            >>> options = IVFRankOptions(mode=IVFRankMode.POST)
            >>> options.to_dict()
            {'mode': 'post'}
        """
        return {"mode": self.mode.value}

    def __repr__(self) -> str:
        """Return string representation of IVF rank options."""
        return f"IVFRankOptions(mode={self.mode.value})"

    def __eq__(self, other: Any) -> bool:
        """Check equality with another IVFRankOptions instance."""
        if not isinstance(other, IVFRankOptions):
            return False
        return self.mode == other.mode
