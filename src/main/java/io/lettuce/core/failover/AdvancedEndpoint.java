package io.lettuce.core.failover;

import java.util.Collection;
import java.util.List;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.CompleteableCommand;
import io.lettuce.core.protocol.DefaultEndpoint;
import io.lettuce.core.protocol.RedisCommand;
import io.lettuce.core.resource.ClientResources;

/**
 * Advanced endpoint for multi-database failover with circuit breaker metrics tracking. Extends DefaultEndpoint and tracks
 * command attempts, successes, and failures.
 *
 * @author Augment
 */
public class AdvancedEndpoint extends DefaultEndpoint implements ManagedCommandQueue {

    private CircuitBreaker circuitBreaker;

    public AdvancedEndpoint(ClientOptions clientOptions, ClientResources clientResources) {
        super(clientOptions, clientResources);
    }

    /**
     * Set the circuit breaker for this endpoint. Must be called before any commands are written.
     *
     * @param circuitBreaker the circuit breaker instance
     */
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Get the circuit breaker for this endpoint.
     *
     * @return the circuit breaker instance
     */
    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    @Override
    public <K, V, T> RedisCommand<K, V, T> write(RedisCommand<K, V, T> command) {
        // Track attempt
        if (circuitBreaker != null) {
            circuitBreaker.recordAttempt();
        }

        // Delegate to parent
        RedisCommand<K, V, T> result = super.write(command);

        // Attach completion callback to track success/failure
        if (circuitBreaker != null && result instanceof CompleteableCommand) {
            CompleteableCommand<T> completeable = (CompleteableCommand<T>) result;
            completeable.onComplete((output, error) -> {
                if (error != null) {
                    circuitBreaker.recordFailure(error);
                } else {
                    circuitBreaker.recordSuccess();
                }
            });
        }

        return result;
    }

    @Override
    public <K, V> Collection<RedisCommand<K, V, ?>> write(Collection<? extends RedisCommand<K, V, ?>> commands) {
        // Track each command individually
        if (circuitBreaker != null) {
            for (@SuppressWarnings("unused")
            RedisCommand<?, ?, ?> command : commands) {
                circuitBreaker.recordAttempt();
            }
        }

        // Delegate to parent
        Collection<RedisCommand<K, V, ?>> result = super.write(commands);

        // Attach completion callbacks to track success/failure for each command
        if (circuitBreaker != null) {
            for (RedisCommand<K, V, ?> command : result) {
                if (command instanceof CompleteableCommand) {
                    @SuppressWarnings("unchecked")
                    CompleteableCommand<Object> completeable = (CompleteableCommand<Object>) command;
                    completeable.onComplete((output, error) -> {
                        if (error != null) {
                            circuitBreaker.recordFailure(error);
                        } else {
                            circuitBreaker.recordSuccess();
                        }
                    });
                }
            }
        }

        return result;
    }

    @Override
    public List<RedisCommand<?, ?, ?>> drainCommands() {
        return super.drainCommands();
    }

}
