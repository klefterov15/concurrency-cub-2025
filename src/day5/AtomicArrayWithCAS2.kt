@file:Suppress("DuplicatedCode", "UNCHECKED_CAST")

package day5

import java.util.concurrent.atomic.*

// This implementation never stores `null` values.
class AtomicArrayWithCAS2<E : Any>(size: Int, initialValue: E) {
    private val array = AtomicReferenceArray<Any?>(size)

    init {
        // Fill array with the initial value.
        for (i in 0 until size) {
            array[i] = initialValue
        }
    }

    fun get(index: Int): E? {
        while (true) {
            when (val slotValue = array[index]) {
                is CAS2Descriptor<*> -> {
                    val cas2Descriptor = slotValue as CAS2Descriptor<E>
                    cas2Descriptor.complete()
                }
                is DCSSDescriptor<*> -> {
                    val dcssDescriptor = slotValue as DCSSDescriptor<E>
                    dcssDescriptor.complete()
                }
                else -> return slotValue as E?
            }
        }
    }

    fun cas2(
        firstIndex: Int, firstExpected: E?, firstUpdate: E?,
        secondIndex: Int, secondExpected: E?, secondUpdate: E?
    ): Boolean {
        require(firstIndex != secondIndex) { "The indices should be different" }

        val (
            orderedIndex1,
            orderedExpected1,
            orderedUpdate1,
            orderedIndex2,
            orderedExpected2,
            orderedUpdate2
        ) = if (firstIndex < secondIndex) {
            CAS2Params(firstIndex, firstExpected, firstUpdate, secondIndex, secondExpected, secondUpdate)
        } else {
            CAS2Params(secondIndex, secondExpected, secondUpdate, firstIndex, firstExpected, firstUpdate)
        }

        val cas2Descriptor = CAS2Descriptor(
            array = array,
            index1 = orderedIndex1,
            expected1 = orderedExpected1,
            update1 = orderedUpdate1,
            index2 = orderedIndex2,
            expected2 = orderedExpected2,
            update2 = orderedUpdate2
        )

        cas2Descriptor.complete()
        return cas2Descriptor.status.get() == Status.SUCCESS
    }

    private data class CAS2Params<E>(
        val firstIndex: Int,
        val firstExpected: E?,
        val firstUpdate: E?,
        val secondIndex: Int,
        val secondExpected: E?,
        val secondUpdate: E?
    )

    private interface Descriptor {
        fun complete(): Boolean
    }

    private enum class Status {
        UNDECIDED,
        SUCCESS,
        FAILED
    }

    private class CAS2Descriptor<E : Any>(
        val array: AtomicReferenceArray<Any?>,
        val index1: Int,
        val expected1: E?,
        val update1: E?,
        val index2: Int,
        val expected2: E?,
        val update2: E?
    ) : Descriptor {

        val status = AtomicReference(Status.UNDECIDED)

        override fun complete(): Boolean {
            val currentStatus = status.get()
            if (currentStatus != Status.UNDECIDED) {
                applyPhysicalUpdate(index1)
                applyPhysicalUpdate(index2)
                return currentStatus == Status.SUCCESS
            }

            if (!installDescriptorAt(index1, expected1)) {
                status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                array.compareAndSet(index1, this, expected1)
                return false
            }

            val dcssInstalled = installWithDCSS(index2, expected2)
            if (!dcssInstalled) {
                status.compareAndSet(Status.UNDECIDED, Status.FAILED)
                applyPhysicalUpdate(index1)
                applyPhysicalUpdate(index2)
                return false
            }

            status.compareAndSet(Status.UNDECIDED, Status.SUCCESS)

            applyPhysicalUpdate(index1)
            applyPhysicalUpdate(index2)

            return status.get() == Status.SUCCESS
        }

        private fun installDescriptorAt(targetIndex: Int, expectedValue: E?): Boolean {
            while (true) {
                when (val currentValue = array[targetIndex]) {
                    expectedValue -> {
                        if (array.compareAndSet(targetIndex, expectedValue, this)) {
                            return true
                        }
                    }
                    this -> return true
                    is CAS2Descriptor<*> -> {
                        val otherCas2 = currentValue as CAS2Descriptor<E>
                        otherCas2.complete()
                    }
                    is DCSSDescriptor<*> -> {
                        val otherDcss = currentValue as DCSSDescriptor<E>
                        otherDcss.complete()
                    }
                    else -> return false
                }
            }
        }

        private fun installWithDCSS(targetIndex: Int, expectedValue: E?): Boolean {
            while (true) {
                when (val currentValue = array[targetIndex]) {
                    expectedValue -> {
                        if (status.get() != Status.UNDECIDED) {
                            return status.get() == Status.SUCCESS
                        }

                        val dcssDescriptor = DCSSDescriptor(
                            array = array,
                            index = targetIndex,
                            expected = expectedValue,
                            update = this,
                            statusRef = status,
                            expectedStatus = Status.UNDECIDED
                        )

                        return dcssDescriptor.complete()
                    }
                    this -> return true
                    is CAS2Descriptor<*> -> {
                        val otherCas2 = currentValue as CAS2Descriptor<E>
                        otherCas2.complete()
                    }
                    is DCSSDescriptor<*> -> {
                        val otherDcss = currentValue as DCSSDescriptor<E>
                        otherDcss.complete()
                    }
                    else -> return false
                }
            }
        }

        private fun applyPhysicalUpdate(targetIndex: Int) {
            while (true) {
                val currentValue = array[targetIndex]

                if (currentValue !is CAS2Descriptor<*> || currentValue !== this) {
                    return
                }

                val newValue = when (status.get()) {
                    Status.SUCCESS ->
                        if (targetIndex == index1) update1 else update2
                    else ->
                        if (targetIndex == index1) expected1 else expected2
                }

                if (array.compareAndSet(targetIndex, this, newValue)) {
                    return
                }
            }
        }
    }

    private class DCSSDescriptor<E : Any>(
        val array: AtomicReferenceArray<Any?>,
        val index: Int,
        val expected: E?,
        val update: Any?,
        val statusRef: AtomicReference<Status>,
        val expectedStatus: Status
    ) : Descriptor {

        val status = AtomicReference(Status.UNDECIDED)

        override fun complete(): Boolean {
            val currentStatus = status.get()
            if (currentStatus != Status.UNDECIDED) {
                applyPhysicalUpdate()
                return currentStatus == Status.SUCCESS
            }

            if (!installSelf()) {
                status.set(Status.FAILED)
                return false
            }

            val referencedStatus = statusRef.get()
            val success = referencedStatus == expectedStatus

            status.compareAndSet(
                Status.UNDECIDED,
                if (success) Status.SUCCESS else Status.FAILED
            )

            applyPhysicalUpdate()

            return status.get() == Status.SUCCESS
        }

        private fun installSelf(): Boolean {
            while (true) {
                when (val currentValue = array[index]) {
                    expected -> {
                        if (array.compareAndSet(index, expected, this)) {
                            return true
                        }
                    }
                    this -> return true
                    is CAS2Descriptor<*> -> {
                        val otherCas2 = currentValue as CAS2Descriptor<E>
                        otherCas2.complete()
                    }
                    is DCSSDescriptor<*> -> {
                        val otherDcss = currentValue as DCSSDescriptor<E>
                        otherDcss.complete()
                    }
                    else -> return false
                }
            }
        }

        private fun applyPhysicalUpdate() {
            while (true) {
                val currentValue = array[index]

                if (currentValue !is DCSSDescriptor<*> || currentValue !== this) {
                    return
                }

                val newValue = when (status.get()) {
                    Status.SUCCESS -> update
                    else -> expected
                }

                if (array.compareAndSet(index, this, newValue)) {
                    return
                }
            }
        }
    }
}