import { Message } from 'node-rdkafka'

import { PluginEvent } from '~/plugin-scaffold'

import { EventHeaders, Team } from '../../types'
import { prefetchPersonsStep } from '../../worker/ingestion/event-pipeline/prefetchPersonsStep'
import { PersonsStore } from '../../worker/ingestion/persons/persons-store'
import { CookielessManager } from '../cookieless/cookieless-manager'
import {
    createApplyCookielessProcessingStep,
    createReadonlyRateLimitToOverflowStep,
    createValidateEventMetadataStep,
    createValidateEventPropertiesStep,
} from '../event-preprocessing'
import { createDropOldEventsStep } from '../event-processing/drop-old-events-step'
import { BatchPipelineBuilder } from '../pipelines/builders/batch-pipeline-builders'
import { OverflowRedisRepository } from '../utils/overflow-redirect/overflow-redis-repository'

export interface TestingPostTeamPreprocessingSubpipelineInput {
    message: Message
    headers: EventHeaders
    event: PluginEvent
    team: Team
}

export interface TestingPostTeamPreprocessingSubpipelineConfig {
    personsStore: PersonsStore
    personsPrefetchEnabled: boolean
    cookielessManager: CookielessManager
    overflowTopic: string
    preservePartitionLocality: boolean
    overflowRedisRepository?: OverflowRedisRepository
}

export function createTestingPostTeamPreprocessingSubpipeline<
    TInput extends TestingPostTeamPreprocessingSubpipelineInput,
    TContext,
>(
    builder: BatchPipelineBuilder<TInput, TInput, TContext, TContext>,
    config: TestingPostTeamPreprocessingSubpipelineConfig
) {
    // Compared to post-team-preprocessing-subpipeline.ts:
    // REMOVED: createApplyPersonProcessingRestrictionsStep (applies per-token/distinct_id person processing restrictions)
    // REMOVED: processPersonlessDistinctIdsBatchStep (batch inserts personless distinct IDs)
    // REMOVED: createOverflowLaneTTLRefreshStep (overflow TTL refresh writes to Redis)
    // REMOVED: createPrefetchHogFunctionsStep (no hog transformations in this pipeline)
    // REMOVED: createValidateEventSchemaStep (no event schema enforcement in this pipeline)
    // CHANGED: cookieless uses readonly mode (no identifies/sessions writes to Redis)
    // CHANGED: rate limit uses readonly mode (only checks Redis for already-flagged keys, no rate limiting or flagging)
    return builder
        .sequentially((b) => {
            return b
                .pipe(createValidateEventMetadataStep())
                .pipe(createValidateEventPropertiesStep())
                .pipe(createDropOldEventsStep())
        })
        .gather()
        .pipeBatch(createApplyCookielessProcessingStep(config.cookielessManager))
        .pipeBatch(
            createReadonlyRateLimitToOverflowStep(
                config.overflowTopic,
                config.preservePartitionLocality,
                config.overflowRedisRepository
            )
        )
        .pipeBatch(prefetchPersonsStep(config.personsStore, config.personsPrefetchEnabled))
}
