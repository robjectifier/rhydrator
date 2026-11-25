"""Policies for assigning one RNTuple cluster's pages to storage objects."""

from collections.abc import Callable, Iterable

from .reader import InterpretableColumnRange, InterpretablePage, page_range

type PageReference = tuple[InterpretableColumnRange, InterpretablePage]
type Bundler = Callable[
    [Iterable[InterpretableColumnRange]], Iterable[Iterable[PageReference]]
]


def one_page_per_object(
    column_ranges: Iterable[InterpretableColumnRange],
) -> list[list[PageReference]]:
    """Return one bundle per distinct stored byte range, in first-reference order."""
    references_by_source_range: dict[tuple[int, int], list[PageReference]] = {}
    for column_range in column_ranges:
        for page in column_range.pages:
            source_range = page_range(page)
            references_by_source_range.setdefault(source_range, []).append(
                (column_range, page)
            )
    return list(references_by_source_range.values())


BUNDLERS: dict[str, Bundler] = {"one-page-per-object": one_page_per_object}
