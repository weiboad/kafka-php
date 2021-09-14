<?php
declare(strict_types=1);

namespace KafkaTest\Base\Consumer;

use Kafka\Consumer\State;
use PHPUnit\Framework\TestCase;
use function PHPUnit\Framework\assertSame;

final class StateTest extends TestCase
{
    /**
     * @test
     */
    public function stopShouldNotBreakWhenNoWatchersExist(): void
    {
        $state = State::getInstance();
        $state->init();
        $state->stop();

        assertSame([], $state->getCallStatus());
    }
}
