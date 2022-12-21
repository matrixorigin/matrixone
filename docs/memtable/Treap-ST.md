# Treap-ST

Author: Zeng Yan

Date: 12/21/2022

Issue: https://github.com/matrixorigin/matrixone/issues/7121

## Introduction
Treap-ST is a memtable which can read/write multi-version KV concurrently.

It is a new idea so that every part below still need improvements or may have mistakes.

## ST
ST(Sparse Table) is a singlelist. In a simple list,every node has a pointer called **next** which points to the next node. In a ST, every node has a slice called **[]next**. **next[i]** points to the $2^i$th next node.

For each key, store all versions in a ST and find one with an ordered number such as transaction ID.
```go
now := ST.last
if now == nil || now.ID == ID{
    return now
}
for now.ID > ID {
    for i := now.next.len() - 1; i > 0; i-- {
        if now.next[i].ID >= ID {
            break
        }
    }
    now = now.next[i]
    if now == nil || now.ID < ID {
        return nil
    }
}
return now
```
When you insert a new node, you need to calculate []next
```go
now.next = []*ST{ST.last}
if ST.last != nil {
    for i := 0 ;; i++ {
        next := now.next[i]
        if next.next[i] == nil {
            break
        }
        now.next = append(now.next, next.next[i])
    }
}
ST.last = now
```
One ST can only have one writer. When the writer is writing,it can have many readers. If one reader is reading while the writer is changing **ST.last**, it's safe, because you can always find an existing node with both old **St.last** and new **ST.last**.

Here is a simple undo.
```go
if ST.last != nil {
    ST.last = ST.last.next[0]
}
```
If you needn't to read the last one, it's safe too. 

If you want to do something more, such as delete some nodes in a ST though they are stright, you must scan the whole ST and stop all readers and writers.

There’s no keys stored in a ST, before you go into a ST, you must compare the keys first.

## Treap
Treap means tree and heap, it's a binary tree and it's expectantly nearly balance. We ask each leaf node to store one key and others not. For every node, keys in its left subtree must be less than keys in its right subtree, so that a treap is a tree. Every node have a positive random **weight** while all leaf nodes have **0**. The **weight** of each node should be the greatest one in the whole subtree, so that a treap is a heap. We can do a Rotate to fix it, which is normal in different kinds of binary trees. The expectant height of the whole treap is O(log n), while n means the count of nodes in a treap.

There are three datas at a node, **key**, **weight** and **ST**. When you find a leaf node and the key is matched, you can get the ST of the key. When you find a non-leaf node, if your key is greater than the **key** at the node, you should go into the right subtree, otherwise the left subtree, so that **key** at a non-leaf node means the greatest key in its left subtree.
```go
func (now *Tree) Get(key []byte) *ST{
    if now.weight ==0 {
        if now.equal(key) {
            return now.ST
        }
        return nil
    }
    if now.less(key) {
        return now.right.Get(key)
    }
    return now.left.Get(key)
}
```
When you write, you do the similar thing first. If you find an existing ST, you do ST's insert, otherwise you should create a subtree with three nodes.
```go
func(now *Treap) Set(key []byte, value []byte) {
    if now.weight == 0{
        if now.equal(key) {
            now.ST.Set(value)
            return
        } else {
            now.lock()
            if now.weight == 0 {
                now.Extend(key)
            }
            now.unlock()
        }
    }
    if now.less(key) {
        now.right.Set(key, value)
    } else {
        now.left.Set(key, value)
    }
}

func (now *Treap) Extend(key []byte) {
    now.left = newnode()
    now.right = newnode()
    if now.less(key) {
        now.left.key = now.key
        now.left.ST = now.ST
        now.left.father = now
        now.right.key = key
        now.right.father = now
    } else {
        now.left.key = key
        now.left.father = now
        now.right.key = now.key
        now.right.ST = now.ST
        now.right.father = now
        now.key = key
    }
    now.weight = rand.Int()
    now.report()//report to the rotater
}
```
This function needs a lock at each node, because you can't do two Extend at the same time.

While someone is doing Extend, another can do Get, if you needn't a read and a write with the same key.Because now.ST means the old ST before Extend and now.key will be changed at the end of the Extend.

### Rotater
In a Rotate, some datas on treap will be changed, but you can consider a treap as a path for visiting.

```go
//rotate to left
func (now *Treap) Zig() {
    if now == now.father.left {
        now.father.left = now.right
    } else {
        now.father.right = now.right
    }
    right := now.right
    right.father = now.father
    now.father = right
    now.right = right.left
    right.left = now
    now.right.father = now
}

//rotate to right, similar
func (now *Treap) Zag() {}
```
No one needs to change key in rotating.

We report every extended node to Rotater, instead of do rotating immediately after Set, because there are many writers and readers one the treap. To avoid the possible conflicts, let Rotate do all rotating.

The Rotater get some extended nodes in a period of time, it copy the whole tree and do the rotating. After all, it submits the new root。

It can also Delete a leaf node in this period.

In golang, runtime.LockOSThread() lets the Rotater can be always runing on a thread

## Iterator
Iterator in Treap-ST is naive, you need to start scaning the tree at the position when the iterator +1 or -1. But it's not slow, because you will scan every edge on treap only twice.

That means, Iter.Next() and Iter.Prev() need 4 moves on treap averagely.

## Competitor
https://github.com/tidwall/btree

The target of Treap-ST is faster than it and use less memory.

## APIs
(n means count of different keys, m means conut of versions of one key)

Global memory complexity(treap): 4n * size of treap node

Get(key, version): no memory, time complexity O(log n + log m)

Set(key, value, version): memory complexity O(log m) * size of ST node, time complexity O(log n + log m)

Delete(key): time complexity O(log n)

Iter.First()/Last(): time complexity O(log n + log m)

Iter.Next()/Prev(): time complexity O(4 + log m)

And some other APIs can be achieved.

## Lock
When you Expend on treap or Set on ST, you need a write-lock at that position, while others can read.

In other cases, it's thread safe.

## Development
ST can do merge and split, so that we can merge an inactive ST, and split it when it need a writing, merge and split just update []next and last. But it needs to store keys into ST.

In a simple treap, all weights are const, but if one subtree has too many visitings, weights can be increased to change the structure of treap.

If there is any more requirement, please ask zengyan.

## Test Plan
There will be many readers, maybe more than 1000 or even much more, using both Get() and Iterators.

There will be different numbers of writers concurrently, only one, number of cores when it's running, and more.

There will be different numbers of different keys.
