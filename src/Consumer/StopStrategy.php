<?php
declare(strict_types=1);

namespace Kafka\Consumer;

use Kafka\Consumer;

interface StopStrategy
{
    public function setup(Consumer $consumer): void;
}
