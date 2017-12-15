<?php
declare(strict_types=1);

namespace Kafka\Contracts;

interface AsynchronousProcess
{
    public function stop(): void;
}
