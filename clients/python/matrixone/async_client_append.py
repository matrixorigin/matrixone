from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .async_client import AsyncClient


class AsyncTransactionSimpleFulltextQueryBuilder:
    """Async transaction-aware simple fulltext query builder."""

    def __init__(self, client: "AsyncClient", table_or_columns, transaction_wrapper):
        """Initialize async transaction-aware query builder."""
        # Import here to avoid circular imports
        from .client import SimpleFulltextQueryBuilder

        self._base_builder = SimpleFulltextQueryBuilder(client, table_or_columns)
        self.transaction_wrapper = transaction_wrapper

    def __getattr__(self, name):
        """Delegate all builder methods to the base builder."""
        return getattr(self._base_builder, name)

    def execute(self):
        """This should not be called for async - use async_execute instead."""
        raise RuntimeError("Use async_execute() for async transaction operations")

    async def async_execute(self):
        """Execute the query asynchronously within the transaction."""
        sql = self._base_builder.build_sql()
        return await self.transaction_wrapper.execute(sql)
