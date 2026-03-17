import { HealthCheckResultOk } from '../../../types'
import { OverflowRedisRepository } from './overflow-redis-repository'
import { ReadonlyOverflowRedirect } from './readonly-overflow-redirect'

const createMockRepository = (flaggedKeys: Map<string, boolean> = new Map()): jest.Mocked<OverflowRedisRepository> => ({
    batchCheck: jest.fn().mockResolvedValue(flaggedKeys),
    batchFlag: jest.fn().mockResolvedValue(undefined),
    batchRefreshTTL: jest.fn().mockResolvedValue(undefined),
    healthCheck: jest.fn().mockResolvedValue(new HealthCheckResultOk()),
})

const createBatch = (token: string, distinctId: string, eventCount = 1) => ({
    key: { token, distinctId },
    eventCount,
    firstTimestamp: Date.now(),
})

describe('ReadonlyOverflowRedirect', () => {
    describe('handleEventBatch', () => {
        it('returns empty set for empty batch', async () => {
            const repo = createMockRepository()
            const service = new ReadonlyOverflowRedirect(repo)

            const result = await service.handleEventBatch('events', [])

            expect(result.size).toBe(0)
            expect(repo.batchCheck).not.toHaveBeenCalled()
        })

        it('returns empty set when no keys are flagged', async () => {
            const repo = createMockRepository(
                new Map([
                    ['token1:user1', false],
                    ['token1:user2', false],
                ])
            )
            const service = new ReadonlyOverflowRedirect(repo)

            const batch = [createBatch('token1', 'user1'), createBatch('token1', 'user2')]

            const result = await service.handleEventBatch('events', batch)

            expect(result.size).toBe(0)
        })

        it('returns flagged keys from Redis', async () => {
            const repo = createMockRepository(
                new Map([
                    ['token1:user1', true],
                    ['token1:user2', false],
                    ['token2:user1', true],
                ])
            )
            const service = new ReadonlyOverflowRedirect(repo)

            const batch = [
                createBatch('token1', 'user1'),
                createBatch('token1', 'user2'),
                createBatch('token2', 'user1'),
            ]

            const result = await service.handleEventBatch('events', batch)

            expect(result.size).toBe(2)
            expect(result.has('token1:user1')).toBe(true)
            expect(result.has('token2:user1')).toBe(true)
            expect(result.has('token1:user2')).toBe(false)
        })

        it('never calls batchFlag', async () => {
            const repo = createMockRepository(new Map([['token1:user1', true]]))
            const service = new ReadonlyOverflowRedirect(repo)

            await service.handleEventBatch('events', [createBatch('token1', 'user1')])

            expect(repo.batchFlag).not.toHaveBeenCalled()
        })

        it('never calls batchRefreshTTL', async () => {
            const repo = createMockRepository()
            const service = new ReadonlyOverflowRedirect(repo)

            await service.handleEventBatch('events', [createBatch('token1', 'user1')])

            expect(repo.batchRefreshTTL).not.toHaveBeenCalled()
        })

        it('passes correct keys to batchCheck', async () => {
            const repo = createMockRepository()
            const service = new ReadonlyOverflowRedirect(repo)

            const batch = [createBatch('token1', 'user1'), createBatch('token2', 'user2')]

            await service.handleEventBatch('events', batch)

            expect(repo.batchCheck).toHaveBeenCalledWith('events', [
                { token: 'token1', distinctId: 'user1' },
                { token: 'token2', distinctId: 'user2' },
            ])
        })

        it('passes correct type to batchCheck', async () => {
            const repo = createMockRepository()
            const service = new ReadonlyOverflowRedirect(repo)

            await service.handleEventBatch('recordings', [createBatch('token1', 'session1')])

            expect(repo.batchCheck).toHaveBeenCalledWith('recordings', [{ token: 'token1', distinctId: 'session1' }])
        })
    })

    describe('healthCheck', () => {
        it('returns ok', async () => {
            const repo = createMockRepository()
            const service = new ReadonlyOverflowRedirect(repo)

            const result = await service.healthCheck()

            expect(result).toBeInstanceOf(HealthCheckResultOk)
        })
    })

    describe('shutdown', () => {
        it('does not throw', async () => {
            const repo = createMockRepository()
            const service = new ReadonlyOverflowRedirect(repo)

            await expect(service.shutdown()).resolves.not.toThrow()
        })
    })
})
