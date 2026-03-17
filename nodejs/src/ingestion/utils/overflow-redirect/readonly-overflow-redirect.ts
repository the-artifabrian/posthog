import { HealthCheckResult, HealthCheckResultOk } from '../../../types'
import { OverflowEventBatch, OverflowRedirectService } from './overflow-redirect-service'
import { OverflowRedisRepository, OverflowType } from './overflow-redis-repository'

/**
 * Readonly implementation of overflow redirect.
 *
 * Only checks Redis for already-flagged keys (batchCheck).
 * Does NOT run the rate limiter or flag new keys in Redis.
 *
 * Used by the testing/shadow pipeline to redirect events that the
 * main pipeline has already flagged, without writing any state.
 */
export class ReadonlyOverflowRedirect implements OverflowRedirectService {
    private redisRepository: OverflowRedisRepository

    constructor(redisRepository: OverflowRedisRepository) {
        this.redisRepository = redisRepository
    }

    async handleEventBatch(type: OverflowType, batch: OverflowEventBatch[]): Promise<Set<string>> {
        if (batch.length === 0) {
            return new Set()
        }

        // Only check Redis for already-flagged keys — no rate limiting, no new flags
        const redisResults = await this.redisRepository.batchCheck(
            type,
            batch.map((e) => e.key)
        )

        const toRedirect = new Set<string>()
        for (const [mKey, isFlagged] of redisResults) {
            if (isFlagged) {
                toRedirect.add(mKey)
            }
        }

        return toRedirect
    }

    healthCheck(): Promise<HealthCheckResult> {
        return Promise.resolve(new HealthCheckResultOk())
    }

    shutdown(): Promise<void> {
        return Promise.resolve()
    }
}
