from typing import List, Any, Callable, Iterator, Iterable, Dict, Tuple
import itertools

class Partition:
    """
    Represents a subset of the data with lineage support.
    Allows re-iteration by re-executing the transformation chain.
    """
    def __init__(self, data_source: Any, index: int, parent: 'Partition' = None, operation: Callable = None):
        """
        Args:
            data_source: The raw data (list/iterable) if this is a root partition.
            index: Partition ID.
            parent: Parent partition (for derived partitions).
            operation: Function to apply to parent iterator.
        """
        self._data_source = data_source
        self.index = index
        self.parent = parent
        self.operation = operation
    
    def __iter__(self):
        if self.parent:
            # Re-execute transformation on parent
            return self.operation(iter(self.parent))
        else:
            # Root partition
            return iter(self._data_source)

class PartitionedDataFrame:
    """
    Simulates a distributed DataFrame split across partitions.
    """
    def __init__(self, data: Iterable[Any] = None, num_partitions: int = 2, partitions: List[Partition] = None):
        if partitions is not None:
            self.partitions = partitions
        else:
            # Initial partitioning (materialize to split)
            full_data = list(data)
            self.partitions = []
            chunk_size = (len(full_data) + num_partitions - 1) // num_partitions
            
            for i in range(num_partitions):
                start = i * chunk_size
                end = min(start + chunk_size, len(full_data))
                chunk = full_data[start:end]
                # Root partitions
                self.partitions.append(Partition(chunk, i))
    
    def map(self, f: Callable[[Any], Any]) -> 'PartitionedDataFrame':
        """
        Apply a function to every element in every partition.
        """
        def map_op(parent_iter: Iterator[Any]) -> Iterator[Any]:
            for item in parent_iter:
                yield f(item)

        new_partitions = []
        for p in self.partitions:
            # Create derived partition pointing to parent p
            new_p = Partition(data_source=None, index=p.index, parent=p, operation=map_op)
            new_partitions.append(new_p)
            
        return PartitionedDataFrame(partitions=new_partitions)

    def filter(self, f: Callable[[Any], bool]) -> 'PartitionedDataFrame':
        """
        Filter elements in every partition.
        """
        def filter_op(parent_iter: Iterator[Any]) -> Iterator[Any]:
            for item in parent_iter:
                if f(item):
                    yield item
        
        new_partitions = []
        for p in self.partitions:
            new_p = Partition(data_source=None, index=p.index, parent=p, operation=filter_op)
            new_partitions.append(new_p)
            
        return PartitionedDataFrame(partitions=new_partitions)
        
    def collect(self) -> List[Any]:
        """
        Collect results from all partitions into a single list.
        Uses ThreadPoolExecutor to materialize partitions in parallel.
        """
        import concurrent.futures
        
        def process_partition(p):
            return list(p)
            
        results = []
        # Parallel execution of partitions
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.partitions)) as executor:
            future_to_part = {executor.submit(process_partition, p): p for p in self.partitions}
            
            # Maintain order is tricky with futures, but we usually want order based on partition index?
            # Or just concat. Real Spark doesn't guarantee global order unless sorted.
            # We'll collect as they finish but should ideally respect partition order for determinism in demos.
            
            # Let's map futures back to indices
            part_results = [None] * len(self.partitions)
            for future in concurrent.futures.as_completed(future_to_part):
                p = future_to_part[future]
                part_results[p.index] = future.result()
                
        for res in part_results:
            if res:
                results.extend(res)
                
        return results

    def count(self) -> int:
        count = 0
        for p in self.partitions:
            count += sum(1 for _ in p)
        return count
        
    def repartition(self, n: int) -> 'PartitionedDataFrame':
        """
        Reshuffle data into n partitions.
        Triggers execution (collect) and creates new root partitions.
        """
        all_data = self.collect()
        return PartitionedDataFrame(data=all_data, num_partitions=n)

    def map_partitions(self, f: Callable[[Iterator[Any]], Iterator[Any]]) -> 'PartitionedDataFrame':
        """
        Apply a function to the entire partition iterator.
        """
        # f takes iterator, returns iterator.
        # This matches our operation signature exactly! (roughly)
        
        # But our operation signature usually expects to take parent_iter and yield.
        # f(iter) -> iter.
        # So operation = f
        
        new_partitions = []
        for p in self.partitions:
            new_p = Partition(data_source=None, index=p.index, parent=p, operation=f)
            new_partitions.append(new_p)
        return PartitionedDataFrame(partitions=new_partitions)

    def reduce_by_key(self, f: Callable[[Any, Any], Any]) -> 'PartitionedDataFrame':
        """
        Global shuffle and reduce.
        Returns a new PartitionedDataFrame with arguably 1 or N partitions.
        """
        all_data = self.collect()
        
        # Group by key
        grouped = {}
        for key, value in all_data:
            if key in grouped:
                grouped[key].append(value)
            else:
                grouped[key] = [value]
        
        # Reduce
        reductions = []
        for key, values in grouped.items():
            if not values:
                continue
            res = values[0]
            for v in values[1:]:
                res = f(res, v)
            reductions.append((key, res))
            
        return PartitionedDataFrame(data=reductions, num_partitions=len(self.partitions))

    def groupBy(self, key_func: Callable[[Any], Any]):
        """
        Groups data by key_func.
        In distributed terms, this involves a shuffle.
        """
        from .aggregation import GroupedDataFrame
        
        all_data = self.collect()
        grouped = {}
        
        for item in all_data:
            key = key_func(item)
            if key in grouped:
                grouped[key].append(item)
            else:
                grouped[key] = [item]
                
        return GroupedDataFrame(grouped)
