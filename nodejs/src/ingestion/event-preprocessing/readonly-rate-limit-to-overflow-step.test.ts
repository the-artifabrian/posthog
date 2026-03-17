import { createTestPipelineEvent } from '../../../tests/helpers/pipeline-event'
import { PipelineResultType } from '../pipelines/results'
import { OverflowRedisRepository } from '../utils/overflow-redirect/overflow-redis-repository'
import { RateLimitToOverflowStepInput } from './rate-limit-to-overflow-step'
import { createReadonlyRateLimitToOverflowStep } from './readonly-rate-limit-to-overflow-step'

const createMockEvent = (token: string, distinctId: string, now?: Date): RateLimitToOverflowStepInput => ({
    headers: {
        token,
        distinct_id: distinctId,
        now: now ?? new Date(),
        force_disable_person_processing: false,
        historical_migration: false,
    },
    event: createTestPipelineEvent({ distinct_id: distinctId }),
})

const createMockRedisRepository = (
    flaggedKeys: Map<string, boolean> = new Map()
): jest.Mocked<OverflowRedisRepository> => ({
    batchCheck: jest.fn().mockResolvedValue(flaggedKeys),
    batchFlag: jest.fn().mockResolvedValue(undefined),
    batchRefreshTTL: jest.fn().mockResolvedValue(undefined),
    healthCheck: jest.fn(),
})

describe('createReadonlyRateLimitToOverflowStep', () => {
    describe('when repository is not provided (overflow disabled)', () => {
        it('returns all events as ok', async () => {
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', true, undefined)

            const events = [
                createMockEvent('token1', 'user1'),
                createMockEvent('token1', 'user2'),
                createMockEvent('token2', 'user1'),
            ]

            const results = await step(events)

            expect(results).toHaveLength(3)
            results.forEach((result) => {
                expect(result.type).toBe(PipelineResultType.OK)
            })
        })
    })

    describe('when repository is provided', () => {
        it('returns ok for events not flagged in Redis', async () => {
            const repo = createMockRedisRepository(
                new Map([
                    ['token1:user1', false],
                    ['token1:user2', false],
                ])
            )
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', true, repo)

            const events = [createMockEvent('token1', 'user1'), createMockEvent('token1', 'user2')]

            const results = await step(events)

            expect(results).toHaveLength(2)
            results.forEach((result) => {
                expect(result.type).toBe(PipelineResultType.OK)
            })
        })

        it('redirects events flagged in Redis', async () => {
            const repo = createMockRedisRepository(
                new Map([
                    ['token1:user1', true],
                    ['token1:user2', false],
                ])
            )
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', true, repo)

            const events = [createMockEvent('token1', 'user1'), createMockEvent('token1', 'user2')]

            const results = await step(events)

            expect(results).toHaveLength(2)
            expect(results[0].type).toBe(PipelineResultType.REDIRECT)
            if (results[0].type === PipelineResultType.REDIRECT) {
                expect(results[0].reason).toBe('rate_limit_exceeded')
                expect(results[0].topic).toBe('overflow_topic')
            }
            expect(results[1].type).toBe(PipelineResultType.OK)
        })

        it('never calls batchFlag (no writes)', async () => {
            const repo = createMockRedisRepository(new Map([['token1:user1', true]]))
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', true, repo)

            const events = [createMockEvent('token1', 'user1')]

            await step(events)

            expect(repo.batchFlag).not.toHaveBeenCalled()
            expect(repo.batchRefreshTTL).not.toHaveBeenCalled()
        })

        it('calls batchCheck with correct keys', async () => {
            const repo = createMockRedisRepository()
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', true, repo)

            const events = [
                createMockEvent('token1', 'user1'),
                createMockEvent('token1', 'user1'),
                createMockEvent('token2', 'user2'),
            ]

            await step(events)

            // The readonly service should call batchCheck with unique keys from the batch
            expect(repo.batchCheck).toHaveBeenCalledTimes(1)
            expect(repo.batchCheck).toHaveBeenCalledWith('events', [
                { token: 'token1', distinctId: 'user1' },
                { token: 'token2', distinctId: 'user2' },
            ])
        })

        it('redirects all events for a flagged key', async () => {
            const repo = createMockRedisRepository(new Map([['token1:user1', true]]))
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', true, repo)

            const events = Array.from({ length: 5 }, () => createMockEvent('token1', 'user1'))

            const results = await step(events)

            expect(results).toHaveLength(5)
            results.forEach((result) => {
                expect(result.type).toBe(PipelineResultType.REDIRECT)
            })
        })

        it('preserves partition key when preservePartitionLocality is true', async () => {
            const repo = createMockRedisRepository(new Map([['token1:user1', true]]))
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', true, repo)

            const events = [createMockEvent('token1', 'user1')]

            const results = await step(events)

            expect(results[0].type).toBe(PipelineResultType.REDIRECT)
            if (results[0].type === PipelineResultType.REDIRECT) {
                expect(results[0].preserveKey).toBe(true)
            }
        })

        it('does not preserve partition key when preservePartitionLocality is false', async () => {
            const repo = createMockRedisRepository(new Map([['token1:user1', true]]))
            const step = createReadonlyRateLimitToOverflowStep('overflow_topic', false, repo)

            const events = [createMockEvent('token1', 'user1')]

            const results = await step(events)

            expect(results[0].type).toBe(PipelineResultType.REDIRECT)
            if (results[0].type === PipelineResultType.REDIRECT) {
                expect(results[0].preserveKey).toBe(false)
            }
        })
    })
})
