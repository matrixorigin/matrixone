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
SQLAlchemy adapters for custom filter objects.

This module provides generic adapters for SQLAlchemy's logical operators (and_, or_, not_)
to work seamlessly with custom filter objects that have a compile() method.

These adapters allow mixing custom filter objects with regular SQLAlchemy expressions
in logical operations.
"""

from typing import Any, Union

from sqlalchemy import and_, false, not_, or_, text, true
from sqlalchemy.sql.elements import ClauseElement

# SQLAlchemy boolean constants for handling empty conditions
_TRUE_CONDITION = true()
_FALSE_CONDITION = false()


class CustomFilterMixin:
    """
    Mixin interface for custom filter objects that can be used with logical adapters.

    Any custom filter class that implements a compile() method can be used with
    the logical adapters in this module.
    """

    def compile(self, **kwargs) -> str:
        """
        Compile the filter to a SQL string.

        Returns:

            SQL string representation of the filter
        """
        raise NotImplementedError("Subclasses must implement compile() method")


def logical_and(*conditions: Union[Any, CustomFilterMixin]) -> ClauseElement:
    """
    Create AND expressions that support custom filter objects.

    This is a generic wrapper around SQLAlchemy's and_() that can handle any objects
    with a compile() method alongside regular SQLAlchemy expressions.

    Args:

        *conditions: Mix of custom filter objects (with compile() method)
                    and regular SQLAlchemy expressions

    Returns:

        SQLAlchemy expression that can be used with filter()

    Example:

        query.filter(logical_and(
            custom_filter.some_condition(),
            Article.category == "Programming",
            another_custom_filter.other_condition()
        ))
    """
    processed_conditions = []
    for condition in conditions:
        if hasattr(condition, 'compile') and callable(getattr(condition, 'compile')):
            # Wrap each custom filter condition in parentheses for proper grouping
            processed_conditions.append(text(f"({condition.compile()})"))
        else:
            processed_conditions.append(condition)

    # Handle empty conditions to avoid SQLAlchemy deprecation warning
    if not processed_conditions:
        return and_(_TRUE_CONDITION)  # Use true() as a neutral condition

    return and_(*processed_conditions)


def logical_or(*conditions: Union[Any, CustomFilterMixin]) -> ClauseElement:
    """
    Create OR expressions that support custom filter objects.

    This is a generic wrapper around SQLAlchemy's or_() that can handle any objects
    with a compile() method alongside regular SQLAlchemy expressions.

    Args:

        *conditions: Mix of custom filter objects (with compile() method)
                    and regular SQLAlchemy expressions

    Returns:

        SQLAlchemy expression that can be used with filter()

    Example:

        query.filter(logical_or(
            custom_filter.condition_a(),
            custom_filter.condition_b(),
            Article.status == "published"
        ))
    """
    processed_conditions = []
    for condition in conditions:
        if hasattr(condition, 'compile') and callable(getattr(condition, 'compile')):
            # Wrap each custom filter condition in parentheses for proper grouping
            processed_conditions.append(text(f"({condition.compile()})"))
        else:
            processed_conditions.append(condition)

    # Handle empty conditions to avoid SQLAlchemy deprecation warning
    if not processed_conditions:
        return or_(_FALSE_CONDITION)  # Use false() as a neutral condition for OR

    return or_(*processed_conditions)


def logical_not(condition: Union[Any, CustomFilterMixin]) -> ClauseElement:
    """
    Create NOT expressions that support custom filter objects.

    This is a generic wrapper around SQLAlchemy's not_() that can handle objects
    with a compile() method alongside regular SQLAlchemy expressions.

    Args:

        condition: A custom filter object (with compile() method) or regular SQLAlchemy expression

    Returns:

        SQLAlchemy expression that can be used with filter()

    Example:

        query.filter(logical_not(
            custom_filter.some_condition()
        ))
    """
    if hasattr(condition, 'compile') and callable(getattr(condition, 'compile')):
        # Wrap the custom filter condition in parentheses for proper grouping
        return text(f"NOT ({condition.compile()})")
    else:
        return not_(condition)


# Convenience aliases for shorter usage
and_adapter = logical_and
or_adapter = logical_or
not_adapter = logical_not
