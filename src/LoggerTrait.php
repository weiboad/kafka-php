<?php
namespace Kafka;

use Psr\Log\LoggerAwareTrait;
use Psr\Log\NullLogger;

trait LoggerTrait
{
    use \Psr\Log\LoggerTrait;
    use LoggerAwareTrait;
    /**
     * Logs with an arbitrary level.
     *
     * @param mixed  $level
     * @param string $message
     * @param array  $context
     *
     * @return void
     */
    public function log($level, $message, array $context = [])
    {
        if ($this->logger == null) {
            $this->logger = new NullLogger();
        }
        $this->logger->log($level, $message, $context);
    }
}
