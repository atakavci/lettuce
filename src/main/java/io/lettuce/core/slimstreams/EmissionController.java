package io.lettuce.core.slimstreams;

/**
 * Simple emission controller that receives demand and decides whether to accept it.
 * <p>
 * When demand is received:
 * <ul>
 * <li>Returns the accepted amount (can be less than requested)</li>
 * <li>Returns 0 if not willing to emit</li>
 * <li>Publisher handles the actual emission via the provided sink</li>
 * </ul>
 *
 * @param <T> the type of elements to emit
 * @author Ali TAKAVCI
 * @since 7.6.0
 */
@FunctionalInterface
public interface EmissionController<T> {

    /**
     * Called when demand is received from a subscriber.
     * <p>
     * The controller should:
     * <ul>
     * <li>Return the amount it's willing to accept (can be less than requested)</li>
     * <li>Return 0 if not willing to emit right now</li>
     * <li>Optionally emit elements via the provided sink</li>
     * </ul>
     *
     * @param requestedAmount the amount of demand requested by the subscriber
     * @param sink the sink to emit elements to the subscriber
     * @return the accepted amount (0 to requestedAmount), 0 means not willing to emit
     */
    long onDemand(long requestedAmount, EmissionSink<T> sink);

}
