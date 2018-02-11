<?php
declare(strict_types=1);

namespace Kafka\Exception;

use Kafka\Exception;
use function sprintf;

final class InvalidRecordInSet extends Exception
{
    public static function missingTopic(): self
    {
        return new self('You have to set "topic" to your message.');
    }

    public static function nonExististingTopic(string $topic): self
    {
        return new self(sprintf('Requested topic "%s" does not exist. Did you forget to create it?', $topic));
    }

    /**
     * FIXME: kill with 🔥
     */
    public static function topicIsNotString(): self
    {
        return new self('Topic must be string.');
    }

    public static function missingValue(): self
    {
        return new self('You have to set "value" to your message.');
    }

    /**
     * FIXME: kill with 🔥
     */
    public static function valueIsNotString(): self
    {
        return new self('Value must be string.');
    }
}
