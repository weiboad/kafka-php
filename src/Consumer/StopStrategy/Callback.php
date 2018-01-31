<?php
namespace Kafka\Consumer\StopStrategy;

use Kafka\Loop;
use Kafka\Consumer;
use Kafka\Consumer\StopStrategy;

final class Callback implements StopStrategy
{
    const DEFAULT_INTERVAL = 250;

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

    public function __construct($callback, $interval = self::DEFAULT_INTERVAL)
    {
        $this->callback = $callback;
        $this->interval = $interval;
        $this->loop     = Loop::getInstance();
    }

    public function setup(Consumer $consumer)
    {
        $this->loop->repeat(
            $this->interval,
            function ($watcherId) use ($consumer) {
                $shouldStop = (bool) call_user_func($this->callback);

                if (! $shouldStop) {
                    return;
                }

                $consumer->stop();
                $this->loop->cancel($watcherId);
            }
        );
    }
}
