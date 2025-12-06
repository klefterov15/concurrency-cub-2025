package day1

import java.util.concurrent.atomic.AtomicReference

class TreiberStack<E> : Stack<E> {
    // Initially, the stack is empty.
    private val top = AtomicReference<Node<E>?>(null)

    override fun push(element: E) {
        while (true) {
           val cur = top.get()
           val new = Node(element, cur)
           if (top.compareAndSet (cur, new)) {
               return
           }
        }
    }

    override fun pop(): E? {
        while (true) {
            val curTop = top.get() ?: return null
            val new = curTop.next
            if (top.compareAndSet(curTop, new)) {
                return curTop.element
            }
        }
    }

    private class Node<E>(
        val element: E,
        val next: Node<E>?
    )
}