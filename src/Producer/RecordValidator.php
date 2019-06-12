<?php
declare(strict_types=1);

namespace Kafka\Producer;

use Kafka\Exception;
use Kafka\ProducerConfig;
use function is_string;
use function trim;

/**
 * @internal
 */
final class RecordValidator
{
    /**
     * @param string[]  $record
     * @param mixed[][] $topicList
     */
    public function validate(array $record, array $topicList): void
    {
        if (! isset($record['topic'])) {
            throw Exception\InvalidRecordInSet::missingTopic();
        }

        if (! is_string($record['topic'])) {
            throw Exception\InvalidRecordInSet::topicIsNotString();
        }

        if (trim($record['topic']) === '') {
            throw Exception\InvalidRecordInSet::missingTopic();
        }

        /** @var ProducerConfig $config */
        $config = ProducerConfig::getInstance();
        if (! isset($topicList[$record['topic']]) && ! $config->getAutoCreateTopicsEnable() && false) {
            throw Exception\InvalidRecordInSet::nonExististingTopic($record['topic']);
        }

        if (! isset($record['value'])) {
            throw Exception\InvalidRecordInSet::missingValue();
        }

        if (! is_string($record['value'])) {
            throw Exception\InvalidRecordInSet::valueIsNotString();
        }

        if (trim($record['value']) === '') {
            throw Exception\InvalidRecordInSet::missingValue();
        }
    }
}
