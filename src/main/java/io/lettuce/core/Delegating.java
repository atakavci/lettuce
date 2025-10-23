package io.lettuce.core;

public interface Delegating<T> {

    T getDelegate();

    default T unwrap() {
        T delegate = getDelegate();
        if (delegate instanceof Delegating) {
            @SuppressWarnings("unchecked")
            T unwrapped = ((Delegating<T>) delegate).unwrap();
            return unwrapped;
        }
        return delegate;
    }

}
