package io.lettuce.core.slimstreams;

/**
 * A sink interface for emitting elements from a publisher.
 * <p>
 * This provides a callback mechanism for the publisher to emit elements
 * when it's ready and willing to do so, and to signal completion or errors.
 * <p>
 * This interface follows the well-established pattern used by RxJava's Emitter,
 * Reactor's FluxSink, and Java 9's Flow API, allowing emission controllers to:
 * <ul>
 * <li>Emit elements via {@link #emit(Object)}</li>
 * <li>Signal completion via {@link #complete()}</li>
 * <li>Signal errors via {@link #error(Throwable)}</li>
 * </ul>
 *
 * @param <T> the type of elements to emit
 * @author Lettuce Contributors
 * @since 7.6.0
 */
public interface EmissionSink<T> {

    /**
     * Emits an element to the subscriber.
     *
     * @param element the element to emit
     * @return {@code true} if the element was emitted, {@code false} otherwise
     */
    boolean emit(T element);

    /**
     * Signals completion of the emission stream.
     * <p>
     * After calling this method, no more elements should be emitted.
     * This is typically called by emission controllers when they have
     * finished generating all elements (e.g., data source exhausted).
     */
    void complete();

    /**
     * Signals an error in the emission stream.
     * <p>
     * After calling this method, no more elements should be emitted.
     * This is typically called by emission controllers when they encounter
     * an error while generating elements.
     * 
     * @param t the error to signal
     */
    void error(Throwable t);

}
