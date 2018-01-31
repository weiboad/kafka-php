<?php
namespace KafkaTest\Functional;

use Kafka\Protocol;

final class AsyncProducerTest extends ProducerTest
{
    /**
     * @test
     *
     * @runInSeparateProcess
     */
    public function sendAsyncMessages()
    {
        $this->configureProducer();

        $messagesSent = false;
        $error        = null;

        $producer = new \Kafka\Producer([$this, 'createMessages']);
        $producer->success(
            function () use (&$messagesSent) {
                $messagesSent = true;
            }
        );
        $producer->error(
            function (int $errorCode) use (&$error) {
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
