<?php
namespace KafkaTest\Functional;

final class SyncProducerTest extends ProducerTest
{
    /**
     * @test
     *
     * @runInSeparateProcess
     */
    public function sendSyncMessages()
    {
        $this->configureProducer();

        $producer = new \Kafka\Producer();
        $messages = $this->createMessages();

        foreach ($messages as $message) {
            $result = $producer->send([$message]);

            self::assertNotEmpty($result);
        }
    }
}
