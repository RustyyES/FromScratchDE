from typing import Callable, Any, Iterator, List, Optional, Iterable

class LazyDataFrame:
    """
    A DataFrame-like class that implements lazy evaluation.
    Transformations (map, filter) are not executed immediately but recorded.
    Actions (collect, count, show) trigger the execution of the transformation chain.
    """
    def __init__(self, data: Optional[Iterable[Any]] = None, parent: Optional['LazyDataFrame'] = None, operation: Optional[Callable] = None):
        """
        Initialize a LazyDataFrame.
        
        Args:
            data: The initial data source (iterable). Only used for the root LazyDataFrame.
            parent: The parent LazyDataFrame in the lineage.
            operation: The operation (generator function) that transforms the parent's data to this one.
        """
        self.data = data
        self.parent = parent
        self.operation = operation

    def __iter__(self) -> Iterator[Any]:
        """
        Execute the lineage chain and yield results lazily.
        """
        if self.parent:
            # If we have a parent, iterate over it and apply our operation
            return self.operation(self.parent)
        else:
            # Root node: just iterate over the data
            return iter(self.data)

    def map(self, f: Callable[[Any], Any]) -> 'LazyDataFrame':
        """
        Transformation: Apply a function to each element.
        Returns a new LazyDataFrame.
        """
        def map_op(parent_iter: Iterable[Any]) -> Iterator[Any]:
            for item in parent_iter:
                yield f(item)
        
        return LazyDataFrame(parent=self, operation=map_op)

    def filter(self, f: Callable[[Any], bool]) -> 'LazyDataFrame':
        """
        Transformation: Filter elements based on a predicate.
        Returns a new LazyDataFrame.
        """
        def filter_op(parent_iter: Iterable[Any]) -> Iterator[Any]:
            for item in parent_iter:
                if f(item):
                    yield item
                    
        return LazyDataFrame(parent=self, operation=filter_op)

    def collect(self) -> List[Any]:
        """
        Action: Execute the entire chain and return results as a list.
        """
        return list(self)

    def count(self) -> int:
        """
        Action: Execute the chain and return the number of elements.
        """
        return sum(1 for _ in self)

    def show(self, n: int = 20) -> None:
        """
        Action: Execute the chain and print the first n elements.
        """
        print(f"+--- Showing top {n} rows ---+")
        for i, item in enumerate(self):
            if i >= n:
                break
            print(item)
        print("+--------------------------+")
