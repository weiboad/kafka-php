<?php
declare(strict_types=1);

namespace Kafka\Contracts\Consumer;

use Kafka\Consumer;

interface Process
{
    public function start(): void;
    public function stop(): void;
}
