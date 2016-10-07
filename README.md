There are 2 datasets:
* events, each containing associated IP address
* mapping company_id -> (first_ip, last_ip, company_priority)
The assumption is that each company has single IP range.
However, ranges might overlap.

Both of those datasets might be large. 
Events might be possible unlimited.
Number of companies theoretically also can be unlimited (number of addresses is limited, but companies can share and repeat ranges)

## PURE SPARK, very simple

#### Assumptions:
1. There is a 1:1 mapping between company ids and IP ranges

I can think of at least one way how it can be done - using SparkSQL. I'd have to be tested whether it's optimal and scalable enough - if it is there is no reason to go with more complex solution.

#### Using SparkSQL:
1. Perform left outer join of events with ranges using condition START <= EVENT_IP <= END.
2. Group by IP address
3. Reduce each group by choosing in each reduction step row with lower priority

## SPARK + CASSANDRA SOLUTION, more complex

#### Assumptions:
1. Number of events is significantly larger than number of companies.
2. Mapping is fairly stable - neither range, nor priority nor the set of companies changes often.
3. IPs in events are varied, NUMBER_OF_UNIQUE_IPS >> NUMBER_OF_SCOPES
4. There is a 1:1 mapping between company ids and IP ranges

Since the ranges might overlap, we cannot decide to which company given IP should be assigned without analyzing the whole mapping. The most efficient structure for that would be map ip_address -> company_id with O(1) retrieval. However, it would be impractical, given the fact that mapping might be large. The next close thing is map of starting IPs for each range and company_id corresponding to it - which means that overlapping ranges must be extracted and resolved, taking priorities into account. This must to work must support appropriate matches, like SortedMap in Java.

Under abovementioned assumptions it'd be practical to callculate such a mapping beforehand and then reuse it while processing events. Storage technology depends on the actual size of the mapping. With raising dataset size: SortedMap implementation or Redis if needed to be accessible from multiple nodes, Cassandra/HBase if we're talking data which doesn't fit into memory of a single machine.

With such precomputed map, for each event we just need to consult our mapping. If the mapping isn't stored locally, we might use some caching layer to reduce number of roundtrips.

So we have two steps:
1. Calculate mapping
2. Label events, possibly using cache for already computed IPs.

#### More about mapping
After taking priorities into accounts, we should have IP sub-ranges, each one corresponding to single company.

#### How to compute it?
We iterate over company_ids and incrementally apply changes to the mapping - solution is unique, so order of iteration is not important. While taking into account N+1's company_id we might create between zero and two new ranges (by splitting first and last range that our range crosses) and change assignment of any range between them.
This means that we have to make N passes (N being number of company_ids) with (pesimistically) first 1, then 3, then 5... regions each /((1 + 2N-1)/2)*N in total/, which gives at most N^2 updates (if all ranges are overlapping with inverted sorting).

Of course ranges might contain large number of records, but fortunatelly we can process them in batches.
This optimization might cost by itself O(N^2) where N is number of ranges. It's usefullness depends on how varied are IPs in events and how deeply 'nested' are the ranges

#### Is Spark a good tool for the job?
It seems impractical to do this calculation in Spark only. It seems that Spark was created for cases when every element of RDD can be processed fairly independently, while here processing N+1 element requires result produced during production of N previous elements. All logic would be during reduction step.

However, for the next step, which looks up IDs in the datastore and assignes them to the events Spark looks perfect for the job. If the mapping is really big, it's stored in external DB, accessible from all executors. If it's small enough, it can be probablly broadcasted.

#### Parallelization
If we want it for performance, or the dataset is just too big.

If we have the precomputed mapping, second step is trivally parallelizable, provided that the mapping is available to all executors. However, the first step can also be parallelized, at cost of iterating whole dataset.
You just decide on the split points, and then iterate over scopes, splitting those that cross parition boundary and assigning respective parts to their own partitions.



