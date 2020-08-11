package com.sme.spark.serialize;

import java.io.Serializable;
import java.util.Comparator;

/**
 * <p>
 * Spark is distributed system and nodes expect serializable objects to transfer them over network. But, for example, {@link Comparator} is not
 * serializable.
 * </p>
 * The implementation fixes the problem.
 * 
 * @param <T> the type of objects that may be compared by this comparator.
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable
{
    /**
     * Create serializable comparator.
     * 
     * @param <T> the type of objects that may be compared by this comparator;
     * @param comparator The comparator;
     * @return Returns serializable comparator.
     */
    static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator)
    {
        return comparator;
    }
}
