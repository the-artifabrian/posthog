import { DateTime } from 'luxon'

import { InternalPerson, TeamId } from '~/types'

import { ReadonlyPersonsStore } from './readonly-persons-store'
import { InternalPersonWithDistinctId, PersonRepository } from './repositories/person-repository'

describe('ReadonlyPersonsStore', () => {
    let store: ReadonlyPersonsStore
    let mockRepo: Record<string, jest.Mock>
    let teamId: TeamId
    let person: InternalPerson

    beforeEach(() => {
        teamId = 1
        person = {
            id: '1',
            team_id: teamId,
            properties: { test: 'test' },
            created_at: DateTime.now(),
            version: 1,
            properties_last_updated_at: {},
            properties_last_operation: {},
            is_user_id: null,
            is_identified: false,
            uuid: 'person-uuid-1',
            last_seen_at: null,
        }

        mockRepo = {
            fetchPerson: jest.fn().mockResolvedValue(person),
            fetchPersonsByDistinctIds: jest.fn().mockResolvedValue([]),
            fetchPersonsByPersonIds: jest.fn().mockResolvedValue([]),
            createPerson: jest.fn(),
            updatePerson: jest.fn(),
            updatePersonAssertVersion: jest.fn(),
            updatePersonsBatch: jest.fn(),
            deletePerson: jest.fn(),
            addDistinctId: jest.fn(),
            moveDistinctIds: jest.fn(),
            addPersonlessDistinctId: jest.fn(),
            addPersonlessDistinctIdForMerge: jest.fn(),
            addPersonlessDistinctIdsBatch: jest.fn(),
            personPropertiesSize: jest.fn(),
            updateCohortsAndFeatureFlagsForMerge: jest.fn(),
            inTransaction: jest.fn(),
            fetchPersonDistinctIds: jest.fn(),
        }

        store = new ReadonlyPersonsStore(mockRepo as unknown as PersonRepository)
    })

    afterEach(() => {
        jest.clearAllMocks()
    })

    describe('fetchForChecking', () => {
        it('delegates to personRepository with useReadReplica: true', async () => {
            const result = await store.fetchForChecking(teamId, 'distinct-1')

            expect(mockRepo.fetchPerson).toHaveBeenCalledWith(teamId, 'distinct-1', { useReadReplica: true })
            expect(result).toEqual(person)
        })

        it('returns null when person is not found', async () => {
            mockRepo.fetchPerson.mockResolvedValue(undefined)

            const result = await store.fetchForChecking(teamId, 'missing')

            expect(result).toBeNull()
        })
    })

    describe('fetchForUpdate', () => {
        it('delegates to personRepository with forUpdate: false', async () => {
            const result = await store.fetchForUpdate(teamId, 'distinct-1')

            expect(mockRepo.fetchPerson).toHaveBeenCalledWith(teamId, 'distinct-1', { forUpdate: false })
            expect(result).toEqual(person)
        })

        it('returns null when person is not found', async () => {
            mockRepo.fetchPerson.mockResolvedValue(undefined)

            const result = await store.fetchForUpdate(teamId, 'missing')

            expect(result).toBeNull()
        })
    })

    describe('prefetchPersons', () => {
        it('fetches persons and caches results', async () => {
            const personWithDistinctId: InternalPersonWithDistinctId = { ...person, distinct_id: 'distinct-1' }
            mockRepo.fetchPersonsByDistinctIds.mockResolvedValue([personWithDistinctId])

            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])

            expect(mockRepo.fetchPersonsByDistinctIds).toHaveBeenCalledWith(
                [{ teamId, distinctId: 'distinct-1' }],
                true
            )
        })

        it('skips already-cached entries on subsequent calls', async () => {
            const personWithDistinctId: InternalPersonWithDistinctId = { ...person, distinct_id: 'distinct-1' }
            mockRepo.fetchPersonsByDistinctIds.mockResolvedValue([personWithDistinctId])

            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])
            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])

            expect(mockRepo.fetchPersonsByDistinctIds).toHaveBeenCalledTimes(1)
        })

        it('fetches new entries while skipping cached ones', async () => {
            const person1: InternalPersonWithDistinctId = { ...person, distinct_id: 'distinct-1' }
            const person2: InternalPersonWithDistinctId = {
                ...person,
                id: '2',
                uuid: 'person-uuid-2',
                distinct_id: 'distinct-2',
            }

            mockRepo.fetchPersonsByDistinctIds.mockResolvedValueOnce([person1])
            mockRepo.fetchPersonsByDistinctIds.mockResolvedValueOnce([person2])

            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])
            await store.prefetchPersons([
                { teamId, distinctId: 'distinct-1' },
                { teamId, distinctId: 'distinct-2' },
            ])

            expect(mockRepo.fetchPersonsByDistinctIds).toHaveBeenCalledTimes(2)
            expect(mockRepo.fetchPersonsByDistinctIds).toHaveBeenLastCalledWith(
                [{ teamId, distinctId: 'distinct-2' }],
                true
            )
        })

        it('is a no-op when all entries are cached', async () => {
            mockRepo.fetchPersonsByDistinctIds.mockResolvedValue([])

            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])
            mockRepo.fetchPersonsByDistinctIds.mockClear()

            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])

            expect(mockRepo.fetchPersonsByDistinctIds).not.toHaveBeenCalled()
        })

        it('is a no-op for empty input', async () => {
            await store.prefetchPersons([])

            expect(mockRepo.fetchPersonsByDistinctIds).not.toHaveBeenCalled()
        })
    })

    describe('no-op methods', () => {
        it('processPersonlessDistinctIdsBatch is a no-op', async () => {
            await expect(
                store.processPersonlessDistinctIdsBatch([{ teamId, distinctId: 'distinct-1' }])
            ).resolves.toBeUndefined()
        })

        it('getPersonlessBatchResult returns undefined', () => {
            expect(store.getPersonlessBatchResult(teamId, 'distinct-1')).toBeUndefined()
        })

        it('reportBatch is a no-op', () => {
            expect(() => store.reportBatch()).not.toThrow()
        })

        it('removeDistinctIdFromCache is a no-op', () => {
            expect(() => store.removeDistinctIdFromCache(teamId, 'distinct-1')).not.toThrow()
        })

        it('flush returns empty array', async () => {
            const result = await store.flush()
            expect(result).toEqual([])
        })
    })

    describe('reset', () => {
        it('clears the prefetch cache', async () => {
            mockRepo.fetchPersonsByDistinctIds.mockResolvedValue([])

            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])
            store.reset()
            await store.prefetchPersons([{ teamId, distinctId: 'distinct-1' }])

            expect(mockRepo.fetchPersonsByDistinctIds).toHaveBeenCalledTimes(2)
        })
    })

    describe('write methods throw', () => {
        it('inTransaction throws', () => {
            expect(() => store.inTransaction('test', async () => {})).toThrow(
                'ReadonlyPersonsStore does not support transactions'
            )
        })

        it('createPerson throws', () => {
            expect(() =>
                store.createPerson(DateTime.now(), {}, {}, {}, teamId, null, false, 'uuid', {
                    distinctId: 'distinct-1',
                })
            ).toThrow('ReadonlyPersonsStore does not support createPerson')
        })

        it('updatePersonForMerge throws', () => {
            expect(() => store.updatePersonForMerge(person, {}, 'distinct-1')).toThrow(
                'ReadonlyPersonsStore does not support updatePersonForMerge'
            )
        })

        it('updatePersonWithPropertiesDiffForUpdate throws', () => {
            expect(() => store.updatePersonWithPropertiesDiffForUpdate(person, {}, [], {}, 'distinct-1')).toThrow(
                'ReadonlyPersonsStore does not support updatePersonWithPropertiesDiffForUpdate'
            )
        })

        it('deletePerson throws', () => {
            expect(() => store.deletePerson(person, 'distinct-1')).toThrow(
                'ReadonlyPersonsStore does not support deletePerson'
            )
        })

        it('addDistinctId throws', () => {
            expect(() => store.addDistinctId(person, 'distinct-1', 1)).toThrow(
                'ReadonlyPersonsStore does not support addDistinctId'
            )
        })

        it('moveDistinctIds throws', () => {
            expect(() => store.moveDistinctIds(person, person, 'distinct-1', undefined, {} as any)).toThrow(
                'ReadonlyPersonsStore does not support moveDistinctIds'
            )
        })

        it('updateCohortsAndFeatureFlagsForMerge throws', () => {
            expect(() => store.updateCohortsAndFeatureFlagsForMerge(teamId, '1', '2', 'distinct-1')).toThrow(
                'ReadonlyPersonsStore does not support updateCohortsAndFeatureFlagsForMerge'
            )
        })

        it('addPersonlessDistinctId throws', () => {
            expect(() => store.addPersonlessDistinctId(teamId, 'distinct-1')).toThrow(
                'ReadonlyPersonsStore does not support addPersonlessDistinctId'
            )
        })

        it('addPersonlessDistinctIdForMerge throws', () => {
            expect(() => store.addPersonlessDistinctIdForMerge(teamId, 'distinct-1')).toThrow(
                'ReadonlyPersonsStore does not support addPersonlessDistinctIdForMerge'
            )
        })

        it('personPropertiesSize throws', () => {
            expect(() => store.personPropertiesSize('1', teamId)).toThrow(
                'ReadonlyPersonsStore does not support personPropertiesSize'
            )
        })

        it('fetchPersonDistinctIds throws', () => {
            expect(() => store.fetchPersonDistinctIds(person, 'distinct-1', undefined, {} as any)).toThrow(
                'ReadonlyPersonsStore does not support fetchPersonDistinctIds'
            )
        })
    })
})
