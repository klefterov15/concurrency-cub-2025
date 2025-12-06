package day1

import java.util.concurrent.*
import java.util.concurrent.atomic.*

open class TreiberStackWithElimination<E> : Stack<E> {
    private val stack = TreiberStack<E>()
    private data class  Offer <E> (val value: E)
    // TODO: Try to optimize concurrent push and pop operations,
    // TODO: synchronizing them in an `eliminationArray` cell.
    private val eliminationArray = AtomicReferenceArray<Any?>(ELIMINATION_ARRAY_SIZE)

    override fun push(element: E) {
        if (tryPushElimination(element)) return
        stack.push(element)
    }

    protected open fun tryPushElimination(element: E): Boolean {
        //TODO("Implement me!")
        // TODO: Choose a random cell in `eliminationArray`
        // TODO: and try to install the element there.
        // TODO: Wait `ELIMINATION_WAIT_CYCLES` loop cycles
        // TODO: in hope that a concurrent `pop()` grabs the
        // TODO: element. If so, clean the cell and finish,
        // TODO: returning `true`. Otherwise, move the cell
        // TODO: to the empty state and return `false`.

        val i = randomCellIndex()
        //val token = Any()
        val offer = Offer (element)

        if (!eliminationArray.compareAndSet(i, CELL_STATE_EMPTY, offer)) {
            return false
        } else {
            repeat (ELIMINATION_WAIT_CYCLES )  {
                if (eliminationArray.compareAndSet(i, CELL_STATE_RETRIEVED, CELL_STATE_EMPTY)) return true
            }
        }

        if (eliminationArray.compareAndSet(i, offer, CELL_STATE_EMPTY)) {
            return false
        } else {
            if (eliminationArray.compareAndSet(i, CELL_STATE_RETRIEVED, CELL_STATE_EMPTY)) return true
        }

        return false
    }

    override fun pop(): E? = tryPopElimination() ?: stack.pop()

    private fun tryPopElimination(): E? {
        //TODO("Implement me!")
        // TODO: Choose a random cell in `eliminationArray`
        // TODO: and try to retrieve an element from there.
        // TODO: On success, return the element.
        // TODO: Otherwise, if the cell is empty, return `null`.

        val i = randomCellIndex()
        val cur = eliminationArray.get(i) ?: return null
        if ( cur === CELL_STATE_RETRIEVED ) return null
        if ( cur is Offer<*> ) {
            if (eliminationArray.compareAndSet(i, cur, CELL_STATE_RETRIEVED)) {
                //eliminationArray.compareAndSet(i, CELL_STATE_RETRIEVED, CELL_STATE_EMPTY)
                @Suppress("UNCHECKED_CAST")
                return (cur as Offer<E>).value
            }
        }
        return null
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(eliminationArray.length())

    companion object {
        private const val ELIMINATION_ARRAY_SIZE = 2 // Do not change!
        private const val ELIMINATION_WAIT_CYCLES = 1 // Do not change!

        // Initially, all cells are in EMPTY state.
        private val CELL_STATE_EMPTY = null

        // `tryPopElimination()` moves the cell state
        // to `RETRIEVED` if the cell contains element.
        private val CELL_STATE_RETRIEVED = Any()
    }
}