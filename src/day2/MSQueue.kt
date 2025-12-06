package day2

import java.util.concurrent.atomic.AtomicReference

class MSQueue<E> : Queue<E> {
    private val head: AtomicReference<Node<E>>
    private val tail: AtomicReference<Node<E>>

    init {
        val dummy = Node<E>(null)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    override fun enqueue(element: E) {
        val new = Node(element)
        while (true) {
            val curTail = tail.get()
            if ( curTail.next.compareAndSet(null, new) ) {
                tail.compareAndSet( curTail,new )
                return
            } else {
                tail.compareAndSet( curTail, curTail.next.get())
            }
        }
    }

    override fun dequeue(): E? {
        while (true) {
            val curHead = head.get()
            val curHeadNext = curHead.next.get() ?: return null
            val curTail = tail.get()
            val element = curHeadNext.element

            if (curHead === curTail) {
                tail.compareAndSet(curTail, curHeadNext)
                continue
            }

            if (head.compareAndSet(curHead,curHeadNext)) {
                curHeadNext.element = null
                return element
            }
        }
    }

    // FOR TEST PURPOSE, DO NOT CHANGE IT.
    override fun validate() {
        check(tail.get().next.get() == null) {
            "At the end of the execution, `tail.next` must be `null`"
        }
        check(head.get().element == null) {
            "At the end of the execution, the dummy node shouldn't store an element"
        }
    }

    private class Node<E>(
        var element: E?
    ) {
        val next = AtomicReference<Node<E>?>(null)
    }
}
