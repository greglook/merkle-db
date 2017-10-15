# Index Tree Algorithms

Index trees are modeled after [B+ trees](https://en.wikipedia.org/wiki/B%2B_tree)
and contain both internal index nodes and leaf partitions. Records in the tree
are sorted by their _id key_, which uniquely identifies each record within
the table.


## Attributes

There are three groups of attributes which are used in the index tree structure.
At the table level, two input parameters control how the data tree is shaped:

- `:merkle-db.index/fan-out`
  An integer which restricts the _maximum_ number of children an index node can
  have. In a tree with fan-out `f`, every index node except the root
  must have between `ceiling(f/2)` and `f` children. The root is allowed to have
  between 2 and `f` children. The fan-out must be at least 4, but is often much
  higher to reduce tree height (default: 256).
- `:merkle-db.partition/limit`
  This limit restricts the maximum number of records a single partition can
  hold. This effectively scales the size of the units of parallelism available
  to batch jobs. In a tree with partition limit `p` every partition must contain
  between `ceiling(p/2)` and `p` records, unless the root is a partition
  (implying that there are fewer than `p` records total).

Inside the tree, every node maintains three bookkeeping properties which define
the scope of the subtree under that node:

- `:merkle-db.record/count`
- `:merkle-db.record/first-key`
- `:merkle-db.record/last-key`

Index nodes additionally use these attributes to store links and information
about their children:

- `:merkle-db.index/height`
- `:merkle-db.index/keys`
- `:merkle-db.index/children`


## Reading Data

Record data can be read from the index tree in two fashions; a retrieval of a
batch of keys for specific records, or a linear scan of some range of the data.

### Batch Reads

In the batch case, a simple recursive algorithm is used to divide up the keys at
each node and forward them towards the child that contains the range the
requested keys are in. Once a partition is reached, the records can be looked up
from the tablets and returned back up the recursion stack.

### Range Scans

For scanning a range of records, the start and end keys (if provided) can be
used to eliminate parts of the tree which do not contain any records in the
range. The recursion continues down the first eligible child at each level until
it reaches a partition, at which point the records are scanned out starting from
the first valid key. This can be modeled as a lazy sequence which only expands a
node once the first record from that subtree is requested.


## Updating Data

Updating an index tree starts with the root node and a batch of changes to
apply to it. The changes may be either insertions supplying new field data for a
certain record key or deletions which remove a record.

The batch-update algorithm has the following goals:

- **Log scaling:** visit `O(log_b(n))` nodes for `n` partitions updated.
- **Minimize garbage:** avoid storing nodes which are not part of the final
  tree.
- **Deduplication:** reuse existing stored nodes where possible.

Note that the first and third points here also imply that an update _should not_
rewrite nodes that are not being changed if they no longer match the tree
parameters. For example, changing the fanout from 4 to 6 means that any index
nodes with only two children are no longer valid. An update which doesn't touch
those nodes would leave them in the tree as-is for a later process to fix-up.

### Empty Tree

In the simplest case, we are updating an empty index tree. This is represented
by a nil tree root.

1. Filter out the tombstone changes, since there's nothing to delete.
2. Divide up the additions into valid partition-sized chunks.
3. Serialize out a partition for each chunk.
4. If there are multiple partitions, build an index tree containing them.

### Single Partition

If the tree has fewer than `:merkle-db.partition/limit` records in it, the tree
will have a single partition node as the root. To update it:

1. Read the full records from the partition.
2. Remove any deleted records.
3. Insert the added records, replacing any existing data.
4. If the updated partition is valid, store it and return the node.
5. If the resulting partition would overflow, divide the records into roughly
   equally sized partitions and build an index from them.

### Index Updates

In the general case, there is a tree of existing nodes including one or more
layers of intermediate index nodes and a collection of partition leaves. The
tree must be carefully updated to preserve the structural parameters. Unlike a
regular B+ tree, which can be extended to batches of inserts or deletes, here we
want to perform batch updates which include _both_ updates and deletes. This
means that some nodes in the tree may need to split while other parts need to
merge, and we need to reconcile these while changing as few nodes as possible.

To understand how this is accomplished, let's consider the update to a generic
individual index node in the tree. What would such a function look like? For
inputs, first we'll need the graph store (so we can store and retrieve nodes)
and a set of tree-level parameters specifying the fan-out, partition limit, and
field families. We will assume these as a given throughout the algorithm.

Next we'll need the data for the node we're operating on, and the sequence of
changes to apply to it. Generally, the changes should fall into the range of
record keys that the index subtree covers. Finally, we'll accept a _carry_
input, which will be explained later.

Initially, the obvious choice for an output is the updated version of the input
node. However, this fails to account for the fact that we may need to split the
node if there are many additions, or merge it if there are many deletions.
Instead, the function will return a _vector_ of nodes, along with an integer
specifying the height of the results. If we input an index node `n` at height
`h` and had no splits or merges, we'd expect to get the result `[h [n']]`.

#### Divide Changes

The first thing to do is to take the set of input record changes and divide them
up according to which child subtree they belong to. Each index node maintains a
list of child links and split keys, so portion out the changes to bucket them by
record key.

The result is a sequence of tuples containing the child and a vector of the
record changes assigned to that child. Children with no changes will appear
with a nil second value.

#### Carry Adoption

The next thing to consider is whether the update to this node has a carried
input. If so, we need to determine whether to adopt the carried elements as
children of the current node or keep carrying them down the subtree. Since
carried inputs have the same form as an update result vector, we can check
whether the height of the carry is one less than the height of the current node.

If so, adopt the elements as children of the current node by prepending them to
the sequence of children and changes as nodes with no changes (as they have
already been applied earlier).

If there is a carried input that is _not_ adoptable because it falls deeper in
the tree than the current node, save it as the first carried input into the
sequence of children.

If there is no carried input, there's nothing to do.

#### Update Children

Begin a loop to build up a new vector of updated child nodes. Recurse into the
first child, using the carry if present. Examine the result - if the height is
the same as the original child, add the elements to the output vector.
Otherwise, carry the result into the update on the next child.

After processing all of the original children, there are four possible
scenarios:
- There are no output children and no final carry. All the records in the
  subtree have been removed, so return nil.
- There are no output children and a final carry. Return the carry output for
  further carrying up the tree.
- There are outputs and no carry. This is the simplest case, and we can just
  use the output children directly.
- There are both outputs and a carry. In this case we need to _carry back_ the
  result and merge it into the last node in the output vector.

In the first two cases we are done with the update to this node and can return
the result. In the latter two we have further processing to do. For carry-backs,
we need to re-run update logic on the final output child. The result will either
be an updated set of direct children or another carry output, in which case the
process repeats until the results are valid or we're reduced back to a carry
output.

#### Update Partitions

Updating partitions actually requires loading data into memory, so the algorithm
constrains the total number of partitions loaded to double the partition limit
`p`. Updated sets of records are buffered as the children are processed,
emitting full partitions where possible, merging in underflowing partitions as
needed. The buffer is filled until at least 150% of the partition limit has been
reached, then a 75% full partition is emitted, leaving (at worst) another 75%
full partition if there are no more adjacent to process.

#### Rebuild Indexes

If the result was a sequence of updated (direct) children, then we can now
construct a new index node to contain them. Compare the number of children
available to the tree's fan-out - if there are not enough children to make a
valid half-full node, return the vector of children for carrying.  Otherwise
divide the children into valid-sized branching groups and build index nodes to
contain them.

At this point we've completed the update algorithm for a single node, the result
of which was either a sequence of one or more index nodes at the original level,
or a sequence of sibling nodes carried up from further down the tree.

#### Promote Root

After recursively applying changes to the root node and unwinding back to the
top of the tree, examine the result.

- If it is nil, the tree was entirely deleted.
- If the result height is negative, we have a sequence of records and must make
  an underflowing partition to hold them as the root node.
- If the result has a single node in it, that is the new root of the tree.
- If it has multiple nodes, the tree root split at some point and we must build
  new layers of index nodes until there is a single root node.

## References

- https://pdfs.semanticscholar.org/85eb/4cf8dfd51708418881e2b5356d6778645a1a.pdf
  Insight: instead of flushing all the updates, select a related subgroup that
  minimizes repeated changes to the same node path.
- http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.219.7269&rep=rep1&type=pdf
  Modern B-Tree Techniques
