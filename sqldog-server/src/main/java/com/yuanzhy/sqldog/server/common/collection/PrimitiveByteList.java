package com.yuanzhy.sqldog.server.common.collection;

import java.util.Arrays;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/7
 */
public class PrimitiveByteList {

    private byte[] array;
    private int firstIndex;
    private int lastIndex;
    private int modCount;

    /**
     * Constructs a new instance of IntList with capacity for ten elements.
     */
    public PrimitiveByteList() {
        this(10);
    }

    /**
     * Constructs a new instance of IntList with the specified capacity.
     *
     * @param capacity the initial capacity of this IntList
     */
    public PrimitiveByteList(final int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException();
        }
        firstIndex = lastIndex = 0;
        array = new byte[capacity];
    }

    /**
     * Adds the specified object at the end of this IntList.
     *
     * @param object the object to add
     * @return true
     */
    public boolean add(final byte object) {
        if (lastIndex == array.length) {
            growAtEnd(1);
        }
        array[lastIndex++] = object;
        modCount++;
        return true;
    }

    public void add(final int location, final byte object) {
        final int size = lastIndex - firstIndex;
        if (0 < location && location < size) {
            if (firstIndex == 0 && lastIndex == array.length) {
                growForInsert(location, 1);
            } else if ((location < size / 2 && firstIndex > 0) || lastIndex == array.length) {
                System.arraycopy(array, firstIndex, array, --firstIndex, location);
            } else {
                final int index = location + firstIndex;
                System.arraycopy(array, index, array, index + 1, size - location);
                lastIndex++;
            }
            array[location + firstIndex] = object;
        } else if (location == 0) {
            if (firstIndex == 0) {
                growAtFront(1);
            }
            array[--firstIndex] = object;
        } else if (location == size) {
            if (lastIndex == array.length) {
                growAtEnd(1);
            }
            array[lastIndex++] = object;
        } else {
            throw new IndexOutOfBoundsException();
        }

        modCount++;
    }

    public void clear() {
        if (firstIndex != lastIndex) {
            Arrays.fill(array, firstIndex, lastIndex, (byte)-1);
            firstIndex = lastIndex = 0;
            modCount++;
        }
    }

    public byte get(final int location) {
        if (0 <= location && location < (lastIndex - firstIndex)) {
            return array[firstIndex + location];
        }
        throw new IndexOutOfBoundsException("" + location);
    }

    private void growAtEnd(final int required) {
        final int size = lastIndex - firstIndex;
        if (firstIndex >= required - (array.length - lastIndex)) {
            final int newLast = lastIndex - firstIndex;
            if (size > 0) {
                System.arraycopy(array, firstIndex, array, 0, size);
            }
            firstIndex = 0;
            lastIndex = newLast;
        } else {
            int increment = size / 2;
            if (required > increment) {
                increment = required;
            }
            if (increment < 12) {
                increment = 12;
            }
            final byte[] newArray = new byte[size + increment];
            if (size > 0) {
                System.arraycopy(array, firstIndex, newArray, 0, size);
                firstIndex = 0;
                lastIndex = size;
            }
            array = newArray;
        }
    }

    private void growAtFront(final int required) {
        final int size = lastIndex - firstIndex;
        if (array.length - lastIndex + firstIndex >= required) {
            final int newFirst = array.length - size;
            if (size > 0) {
                System.arraycopy(array, firstIndex, array, newFirst, size);
            }
            firstIndex = newFirst;
            lastIndex = array.length;
        } else {
            int increment = size / 2;
            if (required > increment) {
                increment = required;
            }
            if (increment < 12) {
                increment = 12;
            }
            final byte[] newArray = new byte[size + increment];
            if (size > 0) {
                System.arraycopy(array, firstIndex, newArray, newArray.length - size, size);
            }
            firstIndex = newArray.length - size;
            lastIndex = newArray.length;
            array = newArray;
        }
    }

    private void growForInsert(final int location, final int required) {
        final int size = lastIndex - firstIndex;
        int increment = size / 2;
        if (required > increment) {
            increment = required;
        }
        if (increment < 12) {
            increment = 12;
        }
        final byte[] newArray = new byte[size + increment];
        final int newFirst = increment - required;
        // Copy elements after location to the new array skipping inserted
        // elements
        System.arraycopy(array, location + firstIndex, newArray, newFirst + location + required, size - location);
        // Copy elements before location to the new array from firstIndex
        System.arraycopy(array, firstIndex, newArray, newFirst, location);
        firstIndex = newFirst;
        lastIndex = size + increment;

        array = newArray;
    }

    public void increment(final int location) {
        if ((0 > location) || (location >= (lastIndex - firstIndex))) {
            throw new IndexOutOfBoundsException("" + location);
        }
        array[firstIndex + location]++;
    }

    public boolean isEmpty() {
        return lastIndex == firstIndex;
    }

    public byte remove(final int location) {
        byte result;
        final int size = lastIndex - firstIndex;
        if ((0 > location) || (location >= size)) {
            throw new IndexOutOfBoundsException();
        }
        if (location == size - 1) {
            result = array[--lastIndex];
            array[lastIndex] = 0;
        } else if (location == 0) {
            result = array[firstIndex];
            array[firstIndex++] = 0;
        } else {
            final int elementIndex = firstIndex + location;
            result = array[elementIndex];
            if (location < size / 2) {
                System.arraycopy(array, firstIndex, array, firstIndex + 1, location);
                array[firstIndex++] = 0;
            } else {
                System.arraycopy(array, elementIndex + 1, array, elementIndex, size - location - 1);
                array[--lastIndex] = 0;
            }
        }
        if (firstIndex == lastIndex) {
            firstIndex = lastIndex = 0;
        }

        modCount++;
        return result;
    }

    public int size() {
        return lastIndex - firstIndex;
    }

    public byte[] toArray() {
        final int size = lastIndex - firstIndex;
        final byte[] result = new byte[size];
        System.arraycopy(array, firstIndex, result, 0, size);
        return result;
    }

    public void addAll(final PrimitiveByteList list) {
        growAtEnd(list.size());
        for (int i = 0; i < list.size(); i++) {
            add(list.get(i));
        }
    }

    public void addAll(byte[] bytes) {
        growAtEnd(bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            add(bytes[i]);
        }
    }

    public void addFirst(byte[] bytes) {
        growAtEnd(bytes.length);
        // TODO performance optimize
        for (int i = bytes.length - 1; i >= 0; i--) {
            add(0, bytes[i]);
        }
    }
}
