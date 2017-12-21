<?php
declare(strict_types=1);

namespace KafkaTest\Functional;

final class SyncProducerTest extends ProducerTest
{
    /**
     * @test
     */
    public function sendMessages(): void
    {
        $producer = $this->container->make(\Kafka\Producer::class, ['producer' => null]);
        $messages = $this->createMessages();

        foreach ($messages as $message) {
            $result = $producer->send([$message]);

            self::assertNotEmpty($result);
        }
    }
}
