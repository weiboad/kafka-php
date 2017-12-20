<?php
declare(strict_types=1);

namespace Kafka\StopStrategy;

use Kafka\Loop;
use Kafka\Contracts\AsynchronousProcess;
use Kafka\Contracts\StopStrategy;

final class Callback implements StopStrategy
{
    private const DEFAULT_INTERVAL = 250;

    /**
     * The verification callback that will be executed to check if the consumer must be stopped or not
     *
     * @var callable
     */
    private $callback;

    /**
     * The time interval, in milliseconds, to wait between executions
     *
     * @var int
     */
    private $interval;
    private $loop;

    public function __construct(callable $callback, int $interval = self::DEFAULT_INTERVAL, Loop $loop)
    {
        $this->callback = $callback;
        $this->interval = $interval;
        $this->loop     = $loop;
    }

    public function setup(AsynchronousProcess $process): void
    {
        $this->loop->repeat(
            $this->interval,
            function (string $watcherId) use ($process): void {
                $shouldStop = (bool) ($this->callback)();

                if (! $shouldStop) {
                    return;
                }

                $process->stop();
                $this->loop->cancel($watcherId);
            }
        );
    }
}
