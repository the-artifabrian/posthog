import { DateTime } from 'luxon'

import { Properties } from '~/plugin-scaffold'

import { InternalPerson, PropertiesLastOperation, PropertiesLastUpdatedAt, Team } from '../../../types'
import { CreatePersonResult, MoveDistinctIdsResult } from '../../../utils/db/db'
import { FlushResult, PersonsStore } from './persons-store'
import { PersonsStoreTransaction } from './persons-store-transaction'
import { InternalPersonWithDistinctId, PersonRepository } from './repositories/person-repository'
import { PersonRepositoryTransaction } from './repositories/person-repository-transaction'

/**
 * Read-only PersonsStore for the testing pipeline.
 *
 * Delegates read operations (fetch, prefetch, personless batch) to the
 * underlying PersonRepository.  All write methods throw immediately,
 * making accidental writes a loud failure instead of a silent no-op.
 */
export class ReadonlyPersonsStore implements PersonsStore {
    constructor(private readonly personRepository: PersonRepository) {}

    // ── reads ────────────────────────────────────────────────────────

    async fetchForChecking(teamId: number, distinctId: string): Promise<InternalPerson | null> {
        const person = await this.personRepository.fetchPerson(teamId, distinctId, { useReadReplica: true })
        return person ?? null
    }

    async fetchForUpdate(teamId: number, distinctId: string): Promise<InternalPerson | null> {
        const person = await this.personRepository.fetchPerson(teamId, distinctId, { forUpdate: false })
        return person ?? null
    }

    // Prefetch cache: teamId:distinctId → InternalPerson | null
    private prefetchCache = new Map<string, Promise<InternalPerson | null>>()

    async prefetchPersons(teamDistinctIds: { teamId: number; distinctId: string }[]): Promise<void> {
        const toFetch = teamDistinctIds.filter(({ teamId, distinctId }) => {
            const key = `${teamId}:${distinctId}`
            return !this.prefetchCache.has(key)
        })

        if (toFetch.length === 0) {
            return
        }

        const resultPromise = this.personRepository.fetchPersonsByDistinctIds(toFetch, true)

        // Populate cache with pending promises so concurrent calls don't re-fetch
        for (const { teamId, distinctId } of toFetch) {
            const key = `${teamId}:${distinctId}`
            this.prefetchCache.set(
                key,
                resultPromise.then((results: InternalPersonWithDistinctId[]) => {
                    const match = results.find((p) => p.team_id === teamId && p.distinct_id === distinctId)
                    return match ?? null
                })
            )
        }

        await resultPromise
    }

    // Personless batch: not used in testing pipeline (step was removed), but
    // required by PersonsStore interface.
    async processPersonlessDistinctIdsBatch(_entries: { teamId: number; distinctId: string }[]): Promise<void> {
        // no-op: testing pipeline does not run the personless batch step
    }

    getPersonlessBatchResult(_teamId: number, _distinctId: string): boolean | undefined {
        return undefined
    }

    // ── no-ops for batch lifecycle ───────────────────────────────────

    reportBatch(): void {
        // no-op
    }

    reset(): void {
        this.prefetchCache.clear()
    }

    removeDistinctIdFromCache(_teamId: number, _distinctId: string): void {
        // no-op
    }

    flush(): Promise<FlushResult[]> {
        return Promise.resolve([])
    }

    // ── writes (all throw) ──────────────────────────────────────────

    inTransaction<T>(_description: string, _transaction: (tx: PersonsStoreTransaction) => Promise<T>): Promise<T> {
        throw new Error('ReadonlyPersonsStore does not support transactions')
    }

    createPerson(
        _createdAt: DateTime,
        _properties: Properties,
        _propertiesLastUpdatedAt: PropertiesLastUpdatedAt,
        _propertiesLastOperation: PropertiesLastOperation,
        _teamId: number,
        _isUserId: number | null,
        _isIdentified: boolean,
        _uuid: string,
        _primaryDistinctId: { distinctId: string; version?: number },
        _extraDistinctIds?: { distinctId: string; version?: number }[],
        _tx?: PersonRepositoryTransaction
    ): Promise<CreatePersonResult> {
        throw new Error('ReadonlyPersonsStore does not support createPerson')
    }

    updatePersonForMerge(
        _person: InternalPerson,
        _update: Partial<InternalPerson>,
        _distinctId: string,
        _tx?: PersonRepositoryTransaction
    ): Promise<[InternalPerson, import('../../../kafka/producer').TopicMessage[], boolean]> {
        throw new Error('ReadonlyPersonsStore does not support updatePersonForMerge')
    }

    updatePersonWithPropertiesDiffForUpdate(
        _person: InternalPerson,
        _propertiesToSet: Properties,
        _propertiesToUnset: string[],
        _otherUpdates: Partial<InternalPerson>,
        _distinctId: string,
        _forceUpdate?: boolean,
        _tx?: PersonRepositoryTransaction
    ): Promise<[InternalPerson, import('../../../kafka/producer').TopicMessage[], boolean]> {
        throw new Error('ReadonlyPersonsStore does not support updatePersonWithPropertiesDiffForUpdate')
    }

    deletePerson(
        _person: InternalPerson,
        _distinctId: string,
        _tx?: PersonRepositoryTransaction
    ): Promise<import('../../../kafka/producer').TopicMessage[]> {
        throw new Error('ReadonlyPersonsStore does not support deletePerson')
    }

    addDistinctId(
        _person: InternalPerson,
        _distinctId: string,
        _version: number,
        _tx?: PersonRepositoryTransaction
    ): Promise<import('../../../kafka/producer').TopicMessage[]> {
        throw new Error('ReadonlyPersonsStore does not support addDistinctId')
    }

    moveDistinctIds(
        _source: InternalPerson,
        _target: InternalPerson,
        _distinctId: string,
        _limit: number | undefined,
        _tx: PersonRepositoryTransaction
    ): Promise<MoveDistinctIdsResult> {
        throw new Error('ReadonlyPersonsStore does not support moveDistinctIds')
    }

    updateCohortsAndFeatureFlagsForMerge(
        _teamID: Team['id'],
        _sourcePersonID: InternalPerson['id'],
        _targetPersonID: InternalPerson['id'],
        _distinctId: string,
        _tx?: PersonRepositoryTransaction
    ): Promise<void> {
        throw new Error('ReadonlyPersonsStore does not support updateCohortsAndFeatureFlagsForMerge')
    }

    addPersonlessDistinctId(_teamId: number, _distinctId: string): Promise<boolean> {
        throw new Error('ReadonlyPersonsStore does not support addPersonlessDistinctId')
    }

    addPersonlessDistinctIdForMerge(
        _teamId: number,
        _distinctId: string,
        _tx?: PersonRepositoryTransaction
    ): Promise<boolean> {
        throw new Error('ReadonlyPersonsStore does not support addPersonlessDistinctIdForMerge')
    }

    personPropertiesSize(_personId: string, _teamId: number): Promise<number> {
        throw new Error('ReadonlyPersonsStore does not support personPropertiesSize')
    }

    fetchPersonDistinctIds(
        _person: InternalPerson,
        _distinctId: string,
        _limit: number | undefined,
        _tx: PersonRepositoryTransaction
    ): Promise<string[]> {
        throw new Error('ReadonlyPersonsStore does not support fetchPersonDistinctIds')
    }
}
