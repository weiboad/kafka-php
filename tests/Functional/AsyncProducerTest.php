<?php
declare(strict_types=1);

namespace KafkaTest\Functional;

use Kafka\Protocol;
use Kafka\StopStrategy\Callback;

final class AsyncProducerTest extends ProducerTest
{
    /**
     * @test
     * @runInSeparateProcess
     */
    public function sendAsyncMessages(): void
    {
        $messagesSent = false;
        $error        = null;

        $executionEnd = new \DateTimeImmutable('+1 minute');
        $stop         =  new Callback(
            function () use (&$messagesSent, $executionEnd): bool {
                return $messagesSent || new \DateTimeImmutable() > $executionEnd;
            }
        );

        $producer = $this->container->make(\Kafka\Producer::class, ['producer' => [$this, 'createMessages'], 'stopStrategy' => $stop]);
        $producer->success(
            function () use (&$messagesSent): void {
                $messagesSent = true;
            }
        );
        $producer->error(
            function (int $errorCode) use (&$error): void {
                $error = $errorCode;
            }
        );
        $producer->send();

        self::assertTrue(
            $messagesSent,
            'It was not possible to send the messages, reason: ' . Protocol::getError($error)
        );
    }
}
