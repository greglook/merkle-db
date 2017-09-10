# Index Tree Algorithms

Index trees are modeled after [B+ trees](https://en.wikipedia.org/wiki/B%2B_tree)
and contain both internal index nodes and leaf partitions. Records in the tree
are sorted by their _id key_, which uniquely identifies each record within
the table.


## Attributes

There are three groups of attributes which are used in the index tree structure.
At the table level, two input parameters control how the data tree is shaped:

- `:merkle-db.index/branching-factor`
  An integer which restricts the _maximum_ number of children an index node can
  have. In a tree with branching factor `b`, every index node except the root
  must have between `ceiling(b/2)` and `b` children. The root is allowed to have
  between 2 and `b` children. The branching factor must be at least 4, but is
  often much higher to reduce fanout (default: 256).
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

In the batch case, a simple recursive algorithm is used to divide up the keys at
each node and forward them towards the child that contains the range the
requested keys are in. Once a partition is reached, the records can be looked up
from the tablets and returned back up the recursion stack.

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

### Empty Tree

In the simplest case, we are updating an empty index tree. This is represented
by a nil input.

1. Filter out the tombstone changes, since there's nothing to delete.
2. Divide up the insertions into valid partition-sized chunks.
3. Serialize out a partition for each chunk.
4. If there are multiple partitions, build an index tree containing them.

### Single Partition

If the tree has fewer than `limit` records in it, the tree will have a single
partition node as the root. To update it:

1. Read the full records from the partition.
2. Remove any deleted records.
3. Insert the added records, replacing any existing data.
4. If the updated partition is valid, store it and return the node.
5. If the resulting partition would overflow, divide the records into roughly
   equally sized partitions and build an index from them.

### Index Updates

The visit to each index node can be broken down into several phases:

1. divide changes
2. adopt carried orphans
3. apply changes to children
4. carry orphans (upward/sideways)
5. redistribute children

In the following diagrams, these symbols are used to represent a tree with
branching-factor of 4:

- `O` - unchanged index node
- `*` - candidate index node
- `+` - persisted index node
- `#` - unchanged partition leaf
- `@` - persisted partition leaf
- `U` - underflowing partition leaf
- `x` - node which has had all children deleted

#### Divide Changes

In the first downward phase, changes at the parent are grouped by the child
which links to the subtree containing the referenced keys.

    >       ..[O]..        [????|?????]
           /       \
          O        .O.
         / \      / | \
        O   O    O  O  O
       /|\ / \  / \/|\/ \
       ### # #  # ##### #

            ...O...             |?????]
           /       \
    >    [O]       .O.     [??|???]
         / \      / | \
        O   O    O  O  O
       /|\  |\  / \/|\/ \
       ###  ##  # ##### #

            ...O...             |?????]
           /       \
          O        .O.        |???]
         / \      / | \
    >  [O]  O    O  O  O   [?|?]
       /|\  |\  / \/|\/ \
       ###  ##  # ##### #

            ...O...             |?????]
           /       \
          O        .O.        |???]
         / \      / | \
        O   O    O  O  O     |?]
       /|\  |\  / \/|\/ \
    >[#]##  ##  # ##### #  [?]

#### Apply Changes

In the **apply** phase, changes are applied to the _children_ to produce
new child nodes. The behavior is slightly different depending on whether the
children of the node are partitions or another layer of index nodes.

Updating partitions actually requires loading data into memory, so the algorithm
constrains the total number of partitions loaded to double the partition limit
`p`. Updated sets of records are buffered as the children are processed,
emitting full partitions where possible, merging in underflowing partitions as
needed. The buffer is filled until at least 150% of the partition limit has been
reached, then a 75% full partition is emitted, leaving (at worst) another 75%
full partition if there are no more adjacent to process.

For index nodes, this call returns an unpersisted candidate node, or nil if the
resulting node would be empty. The candidate may have any number of children,
meaning it can either underflow, be valid, or overflow, depending on the changes
applied.

Here, updates have deleted one partition entirely and created an pair of
partitions, which are merged and written out as a (still underflowing)
partition.

            ...O...
           /       \
          O        .O.
         / \      / | \
        O   O    O  O  O
       /|\  |\  /|\/|\/ \
    > [UxU] ##  ####### #

            ...O...
           /       \
          O        .O.
         / \      / | \
        O   O    O  O  O
        |   |\  /|\/|\/ \
    >  [U]  ##  ####### #

When a node has only a single child, it is 'carried' up the tree recursively
so it can be passed down the next branch for merging into the next branch.

            ...O...
           /       \
          O        .O.
         / \      / | \
    >  [U]  O    O  O  O
            |\  /|\/|\/ \
            # # ####### #

            ...O...
           /       \
    > (U)[O]       .O.
          |       / | \
          O      O  O  O
         / \    /|\/|\/ \
        #   #   ####### #

When an orphaned subtree is being passed to the next branch, and the current
node's height is one more than the subtree root, insert it as a child of the
current node:

            ...O...
           /       \
          O        .O.
          |       / | \
    > (U)[*]     O  O  O
         / \    /|\/|\/ \
        #   #   ####### #

            ...O...
           /       \
          O        .O.
          |       / | \
    >    [*]     O  O  O
         /|\    /|\/|\/ \
        U # #   ####### #

Apply updates to the remaining two partitions, deleting one and creating a
second underflowing partition:

            ...O...
           /       \
          O        .O.
          |       / | \
          *      O  O  O
         /|\    /|\/|\/ \
    >  [U x U]  ####### #

The resulting buffer contains enough records to make a single valid partition:

            ...O...
           /       \
          O        .O.
          |       / | \
          *      O  O  O
          |     /|\/|\/ \
    >    [@]    ####### #

            ...O...
           /       \
    >    [@]       .O.
                  / | \
                 O  O  O
                /|\/|\/ \
                ####### #

    >     (@)[O]
              |
            ..O..
           /  |  \
          O   O   O
         /|\ /|\ / \
         ### ### # #

              O
              |
    >    (@).[O].
           /  |  \
          O   O   O
         /|\ /|\ / \
         ### ### # #

              O
              |
            ..O..
           /  |  \
    > (@)[O]  O   O
         /|\ /|\ / \
         ### ### # #

              O
              |
            ..O..
           /  |  \
    >    [O]  O   O
        //|\ /|\ / \
        @### ### # #


              O
              |
            ..O..
           /  |  \
          O   O   O
        //|\ /|\ / \
    >  @##[U]### # #

             O
             |
           ..O..
          /  |  \
         O   O   O
       //|  /|\ / \
    > @#[@] ### # #

             O
             |
           ..O..
          /  |  \
    >   [*]  O   O
        /|\ /|\ / \
        @#@ ### # #

             O
             |
    >      .[*].
          /  |  \
         *   O   O
        /|\ /|\ / \
        @#@ ### # #

Fast forward, skipping a subtree...

             O
             |
           ..*..
          /  |  \
    >    *   O  [O]
        /|\ /|\ / \
        @#@ ### # #

Insert nodes into right subtree, resulting in splitting multiple partitions.

             O
             |
          ...*...
         /   |   \
        *    O    *
       /|\  /|\ //|\\
    >  @#@  ###[#@@@@]

#### Redistribute Children

In the **distribution** phase, any candidate children which have over or
overflowed must split, merge with a neighbor, or borrow some elements from
one. Afterwards, all children should be at least half full and under the
size limit, unless there is only a single child left.

Consider consecutive runs of invalid candidate nodes; if the total number of
children is at least half the limit, repartition the children into a number
of valid nodes. Otherwise, resolve the last link before the run and add it
to the pool to redistribute. Use the link after if the run includes the
first child.

              O
              |
          ....*....
         /   / \   \
    >   *   O  [*   *]
       /|\ /|\ / \ /|\
       @#@ ### # @ @@@

              *
              |
    >     ...[*]...
         /   / \   \
        *   O   *   *
       /|\ /|\ / \ /|\
       @#@ ### # @ @@@

    >        [*]
              |
          ....*....
         /   / \   \
        *   O   *   *
       /|\ /|\ / \ /|\
       @#@ ### # @ @@@

#### Serialize

Now that we're back at the root, we can serialize out the resulting tree of
index nodes. One branch is reused entirely from the original tree, since it
hasn't been changed. All the partition leaves are serialized already at this
point.

              *
              |
    >     ...[+]...
         /   / \   \
    >  [+]  O  [+] [+]
       /|\ /|\ / \ /|\
       @#@ ### # @ @@@

If the upward recursion reaches a branch with a single child, we're on a path
up to the root, so return the subtree directly, decreasing the height of the
tree.

    >        [*]
              |
          ....+....
         /   / \   \
        +   O   +   +
       /|\ /|\ / \ /|\
       @#@ ### # @ @@@

Resulting balanced tree:

          ....+....
         /   / \   \
        +   O   +   +
       /|\ /|\ / \ /|\
       @#@ ### # @ @@@

#### Edge Case: Final Orphan

What happens if the final child is orphaned?

           ..*..
          /  |  \
    >    *   O  [O]
        /|\ /|\ / \
        @#@ ### # #

           ..*..
          /  |  \
         *   O   O
        /|\ /|\ / \
    >   @#@ ###[x]#

           ..*..
          /  |  \
         *   O   O
        /|\ /|\  |
    >   @#@ ### [#]

           ..*..
          /  |  \
    >    *   O  [#]
        /|\ /|\
        @#@ ###

    >      [*](#)
          /   \
         *     O
        /|\   /|\
        @#@   ###

Higher-level node needs to recognize the orphan and do another pass down the
(now final) child node:

           .*.
          /   \
    >    *    [O](#)
        /|\   /|\
        @#@   ###

           .*.
          /   \
    >    *    [*]
        /|\  // \\
        @#@  ## ##

Say that node had been full:

           .*.
          /   \
    >    *    [*]
        /|\  //|\\
        @#@  #####

           ..*..
          /  |  \
    >    *  [*   *]
        /|\ / \ /|\
        @#@ # # ###

    >      .[*].
          /  |  \
         *   *   *
        /|\ / \ /|\
        @#@ # # ###


## References

https://pdfs.semanticscholar.org/85eb/4cf8dfd51708418881e2b5356d6778645a1a.pdf

Insight: instead of flushing all the updates, select a related subgroup that
minimizes repeated changes to the same node path.
