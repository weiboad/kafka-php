<?php
declare(strict_types=1);

namespace Kafka\StopStrategy;

use Kafka\Loop;
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
    private $loop;

    public function __construct(int $delay, Loop $loop)
    {
        $this->delay = $delay;
        $this->loop  = $loop;
    }

    public function setup(AsynchronousProcess $process): void
    {
        $this->loop->delay(
            $this->delay,
            function () use ($process): void {
                $process->stop();
            }
        );
    }
}
