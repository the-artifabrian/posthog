import { OverflowRedisRepository } from '../utils/overflow-redirect/overflow-redis-repository'
import { ReadonlyOverflowRedirect } from '../utils/overflow-redirect/readonly-overflow-redirect'
import { RateLimitToOverflowStepInput, createRateLimitToOverflowStep } from './rate-limit-to-overflow-step'

/**
 * Readonly version of the rate limit to overflow step.
 *
 * Only checks Redis for keys already flagged by the main pipeline.
 * Does NOT run the rate limiter or flag new keys.
 *
 * Used in the testing/shadow pipeline to redirect events that the
 * main pipeline has already flagged for overflow, without side effects.
 */
export function createReadonlyRateLimitToOverflowStep<T extends RateLimitToOverflowStepInput>(
    overflowTopic: string,
    preservePartitionLocality: boolean,
    overflowRedisRepository?: OverflowRedisRepository
) {
    const readonlyService = overflowRedisRepository ? new ReadonlyOverflowRedirect(overflowRedisRepository) : undefined
    return createRateLimitToOverflowStep<T>(overflowTopic, preservePartitionLocality, readonlyService)
}
