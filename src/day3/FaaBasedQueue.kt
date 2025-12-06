package day3

import day2.*
import java.util.concurrent.atomic.*

class FAABasedQueue<E> : Queue<E> {
    private val initialSegment = Segment(0)
    private val headRef = AtomicReference(initialSegment)
    private val tailRef = AtomicReference(initialSegment)
    private val nextEnqueueIndex = AtomicLong(0)
    private val nextDequeueIndex = AtomicLong(0)

    private val marKer = Any()

    override fun enqueue(element: E) {
        while (true) {
            val index = nextEnqueueIndex.getAndIncrement()
            val segmentId = index / SEGMENT_SIZE
            val cellIndex = (index % SEGMENT_SIZE).toInt()

            val targetSegment = findStartingSegment(tailRef, segmentId)
            advanceTail(targetSegment)

            if (targetSegment.cells.compareAndSet(cellIndex, null, element)) return
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        while (true) {
            if (!isDequeuePossible()) return null

            val index = nextDequeueIndex.getAndIncrement()
            val segmentId = index / SEGMENT_SIZE
            val cellIndex = (index % SEGMENT_SIZE).toInt()

            val targetSegment = findStartingSegment(headRef, segmentId)
            advanceHead(targetSegment)

            val cellArray = targetSegment.cells
            while (true) {
                val currentValue = cellArray.get(cellIndex)
                when {
                    currentValue == null -> {
                        if (cellArray.compareAndSet(cellIndex, null, marKer)) break
                        continue
                    }
                    currentValue === marKer -> {
                        break
                    }
                    else -> {
                        if (cellArray.compareAndSet(cellIndex, currentValue, marKer))
                            return currentValue as E
                        continue
                    }
                }
            }
        }
    }

    private fun findStartingSegment(ref: AtomicReference<Segment>, targetId: Long): Segment {
        val observed = ref.get()
        val safeStart = if (observed.id <= targetId) observed else initialSegment
        return getOrCreateSegment(safeStart, targetId)
    }

    private fun getOrCreateSegment(start: Segment, id: Long): Segment {
        var current = start
        while (current.id < id) {
            val nextSeg = current.next.get()
            current = if (nextSeg == null) {
                val created = Segment(current.id + 1)
                if (current.next.compareAndSet(null, created)) created else current.next.get()!!
            } else nextSeg
        }
        return current
    }

    private fun advanceTail(target: Segment) {
        while (true) {
            val observedTail = tailRef.get()
            if (target.id <= observedTail.id) return

            val nextSegment = observedTail.next.get() ?: run {
                observedTail.next.compareAndSet(null, Segment(observedTail.id + 1))
                observedTail.next.get()
            } ?: return

            if (!tailRef.compareAndSet(observedTail, nextSegment)) return
        }
    }

    private fun advanceHead(target: Segment) {
        while (true) {
            val observedHead = headRef.get()
            if (target.id <= observedHead.id) return
            val nextSegment = observedHead.next.get() ?: return
            if (!headRef.compareAndSet(observedHead, nextSegment)) return
        }
    }

    private fun isDequeuePossible(): Boolean {
        while (true) {
            val before = nextEnqueueIndex.get()
            val currentDeq = nextDequeueIndex.get()
            val after = nextEnqueueIndex.get()
            if (before == after) return currentDeq < before
        }
    }
}


private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2