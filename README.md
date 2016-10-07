There are 2 datasets:
* events, each containing associated IP address
* mapping company_id -> (first_ip, last_ip, company_priority)
The assumption is that each company has single IP range.
However, ranges might overlap.

Both of those datasets might be large:
* Events - possibly unlimited
* Number of companies - theoretically also can be unlimited (number of addresses is limited, but companies
can have overlapping or even identical ranges)

## PURE SPARK, very simple

It uses SparkSQL and is very simple. It'd have to test it on real data to decide if it's performant and scalable enough
- if it is there is no reason to go with more complex solution.

#### Overview:
1. Perform left outer join of events with ranges using condition START <= EVENT_IP <= END.
2. Group by IP address
3. Reduce each group by choosing in each reduction step row with lower priority

## SPARK + CASSANDRA SOLUTION, more complex

#### Assumptions:
1. Number of events is significantly larger than number of companies.
2. Mapping is fairly stable - neither range, nor priority nor the set of companies changes often.
3. IPs in events are varied, NUMBER_OF_UNIQUE_IPS >> NUMBER_OF_SCOPES

Since the ranges might overlap, we cannot decide to which company given IP should be assigned without analyzing
the whole mapping. The most efficient structure for that would be map ip_address -> company_id with O(1) retrieval.
However, it would be impractical, given the fact that mapping might be large. The next close thing is map of range
bounaries, with single company_id assigned to each range. This means that overlapping ranges must be extracted
and resolved, taking priorities into account. For this to work datastore must appropriate matches, like SortedMap in Java.

Under abovementioned assumptions it'd be practical to callculate such a mapping beforehand and then reuse it while
processing events. Storage technology depends on the actual size of the mapping. With raising dataset size:
* SortedMap implementation or Redis if needed to be accessible from multiple nodes,
* Cassandra/HBase if we're talking data which won't fit into memory of a single machine.

With such precomputed map, for each event we just need to consult our mapping. If the mapping isn't stored locally,
we might use some caching layer to reduce number of roundtrips.

#### How to compute it?
We iterate over company_ids and incrementally apply changes to the mapping. While taking into account N+1's company_id
we might create between zero and two new ranges (by splitting first and last range that our range crosses) and change
assignment of any range between them.

This means that we have to make N passes (N being number of company_ids) with (pesimistically) first 1, then 3, then 5...
regions each /((1 + 2N-1)/2)*N in total/, which gives at most N^2 updates (if all ranges are overlapping with inverted sorting).

Of course ranges might contain large number of records, but fortunatelly we can process them in batches.

#### Parallelization
If we want it for performance, or the dataset is just too big.

If we have the precomputed mapping, second step is trivally parallelizable, provided that the mapping is available
to all executors. However, the first step can also be parallelized, at cost of iterating whole dataset.
You just decide on the split points, and then iterate over scopes, splitting those that cross parition boundary and
assigning respective parts to their own partitions.



