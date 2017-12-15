<?php
declare(strict_types=1);

namespace Kafka\Consumer\StopStrategy;

use Amp\Loop;
use Kafka\Consumer;
use Kafka\Contracts\Consumer\StopStrategy;

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

    public function __construct(callable $callback, int $interval = self::DEFAULT_INTERVAL)
    {
        $this->callback = $callback;
        $this->interval = $interval;
    }

    public function setup(Consumer $consumer): void
    {
        Loop::repeat(
            $this->interval,
            function (string $watcherId) use ($consumer): void {
                $shouldStop = (bool) ($this->callback)();

                if (! $shouldStop) {
                    return;
                }

                $consumer->stop();
                Loop::cancel($watcherId);
            }
        );
    }
}
