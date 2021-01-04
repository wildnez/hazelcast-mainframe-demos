package com.hazelcast.certification.domain.internal;

import java.io.Serializable;
import java.util.LinkedList;

public class TransactionQueue<E> extends LinkedList<E> implements Serializable {
    private int capacity = 5;

    // no arg constructor needed for DataSerialization
    public TransactionQueue() {}

    public TransactionQueue(int capacity){
        this.capacity = capacity;
    }

    @Override
    public boolean add(E e) {
        if(size() >= capacity)
            removeFirst();
        return super.add(e);
    }

}
