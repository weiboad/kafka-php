<?php
declare(strict_types=1);

namespace Kafka\Contracts;

interface StopStrategy
{
    public function setup(AsynchronousProcess $process): void;
}
