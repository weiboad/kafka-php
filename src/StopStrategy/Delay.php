<?php
declare(strict_types=1);

namespace Kafka\StopStrategy;

use Amp\Loop;
use Kafka\Contracts\AsynchronousProcess;
use Kafka\Contracts\StopStrategy;

final class Delay implements StopStrategy
{
    /**
     * The amount of time, in milliseconds, to stop the consumer
     *
     * @var int
     */
    private $delay;

    public function __construct(int $delay)
    {
        $this->delay = $delay;
    }

    public function setup(AsynchronousProcess $process): void
    {
        Loop::delay(
            $this->delay,
            function () use ($process): void {
                $process->stop();
            }
        );
    }
}
