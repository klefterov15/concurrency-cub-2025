@file:Suppress("UNCHECKED_CAST")

package day6

import java.util.concurrent.atomic.*

class ConcurrentHashTable<K : Any, V : Any>(initialCapacity: Int) {
    private val table = AtomicReference(Table<K, V>(initialCapacity))

    fun put(key: K, value: V): V? {
        while (true) {
            val currentTable = table.get()
            val result = currentTable.put(key, value)

            if (result === NEEDS_REHASH) {
                resize(currentTable)
            } else {
                return result as V?
            }
        }
    }

    fun get(key: K): V? =
        table.get().get(key)

    fun remove(key: K): V? =
        table.get().remove(key)

    private fun resize(currentTable: Table<K, V>) {
        val newTable = Table<K, V>(currentTable.capacity * 2)

        currentTable.next.compareAndSet(null, newTable)

        for (index in 0 until currentTable.capacity) {
            currentTable.copySlot(index)
        }

        table.compareAndSet(currentTable, currentTable.next.get())
    }

    class Table<K : Any, V : Any>(val capacity: Int) {
        val keys = AtomicReferenceArray<Any?>(capacity)
        val values = AtomicReferenceArray<Any?>(capacity)
        val next = AtomicReference<Table<K, V>?>(null)

        fun put(key: K, value: V): Any? {
            var index = index(key)
            val startIndex = index

            while (true) {
                if (next.get() != null) {
                    copySlot(index)
                }

                when (val keyAtIndex = keys.get(index)) {
                    is KeyValue -> {
                        helpPut(index, keyAtIndex)
                        continue
                    }

                    is MovedKey -> {
                        if (keyAtIndex.key == key) {
                            return next.get()!!.put(key, value)
                        }
                    }

                    null -> {
                        val keyValue = KeyValue(key, value)
                        if (keys.compareAndSet(index, null, keyValue)) {
                            helpPut(index, keyValue)
                            return null
                        }
                        continue
                    }

                    key -> {
                        while (true) {
                            val currentValue = values.get(index)

                            if (currentValue is MovedValue) {
                                copySlot(index)
                                break
                            }

                            if (values.compareAndSet(index, currentValue, value)) {
                                return if (currentValue === TOMBSTONE) {
                                    null
                                } else {
                                    currentValue as V?
                                }
                            }
                        }
                        continue
                    }
                }

                index = (index + 1) % capacity
                if (index == startIndex) return NEEDS_REHASH
            }
        }

        fun get(key: K): V? {
            var index = index(key)
            val startIndex = index

            while (true) {
                if (next.get() != null) {
                    copySlot(index)
                }

                when (val keyAtIndex = keys.get(index)) {
                    is KeyValue -> {
                        helpPut(index, keyAtIndex)
                        continue
                    }

                    is MovedKey -> {
                        if (keyAtIndex.key == key) {
                            return next.get()!!.get(key)
                        }
                    }

                    null -> return null

                    key -> {
                        val valueAtIndex = values.get(index)
                        return when {
                            valueAtIndex === TOMBSTONE -> null
                            valueAtIndex is MovedValue -> valueAtIndex.value as V?
                            else -> valueAtIndex as V?
                        }
                    }
                }

                index = (index + 1) % capacity
                if (index == startIndex) return null
            }
        }

        fun remove(key: K): V? {
            var index = index(key)
            val startIndex = index

            while (true) {
                if (next.get() != null) {
                    copySlot(index)
                }

                when (val keyAtIndex = keys.get(index)) {
                    is KeyValue -> {
                        helpPut(index, keyAtIndex)
                        continue
                    }

                    is MovedKey -> {
                        if (keyAtIndex.key == key) {
                            return next.get()!!.remove(key)
                        }
                    }

                    null -> return null

                    key -> {
                        while (true) {
                            when (val currentValue = values.get(index)) {
                                is MovedValue -> {
                                    copySlot(index)
                                    return next.get()!!.remove(key)
                                }

                                TOMBSTONE -> return null

                                else -> {
                                    if (values.compareAndSet(index, currentValue, TOMBSTONE)) {
                                        return currentValue as V?
                                    }
                                }
                            }
                        }
                    }
                }

                index = (index + 1) % capacity
                if (index == startIndex) return null
            }
        }

        fun copySlot(index: Int) {
            val nextTable = next.get() ?: return

            while (true) {
                when (val keyAtIndex = keys.get(index)) {
                    null -> return

                    is MovedKey -> return

                    is KeyValue -> {
                        helpPut(index, keyAtIndex)
                        continue
                    }

                    else -> {
                        when (val valueAtIndex = values.get(index)) {
                            null, TOMBSTONE -> return

                            is MovedValue -> {
                                val newIndex = nextTable.index(keyAtIndex as K)
                                val keyValue = KeyValue(keyAtIndex, valueAtIndex.value)

                                nextTable.keys.compareAndSet(newIndex, null, keyValue)
                                keys.set(index, MovedKey(keyAtIndex))
                                return
                            }

                            else -> {
                                if (!values.compareAndSet(index, valueAtIndex, MovedValue(valueAtIndex))) {
                                    continue
                                }

                                val newIndex = nextTable.index(keyAtIndex as K)
                                val keyValue = KeyValue(keyAtIndex, valueAtIndex)

                                nextTable.keys.compareAndSet(newIndex, null, keyValue)
                                keys.set(index, MovedKey(keyAtIndex))
                                return
                            }
                        }
                    }
                }
            }
        }

        private fun helpPut(index: Int, keyValue: KeyValue) {
            values.compareAndSet(index, null, keyValue.value)
            keys.compareAndSet(index, keyValue, keyValue.key)
        }

        private fun index(key: K): Int =
            (key.hashCode() and Int.MAX_VALUE) % capacity
    }
}

private val NEEDS_REHASH = Any()
private val TOMBSTONE = Any()
private class KeyValue(val key: Any?, val value: Any?)
private class MovedKey(val key: Any?)
private class MovedValue(val value: Any?)