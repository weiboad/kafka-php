<?php
declare(strict_types=1);

namespace Kafka;

use Consistence\Enum\Enum;

final class ConsumeMode extends Enum
{
    public const CONSUME_AFTER_COMMIT_OFFSET  = 1;
    public const CONSUME_BEFORE_COMMIT_OFFSET = 2;

    public static function consumeAfterCommitOffset(): self
    {
        return self::get(self::CONSUME_AFTER_COMMIT_OFFSET);
    }

    public static function consumeBeforeCommitOffset(): self
    {
        return self::get(self::CONSUME_BEFORE_COMMIT_OFFSET);
    }
}
