<?php
declare(strict_types=1);

namespace KafkaTest\Functional;

use Kafka\Producer;

final class SyncProducerTest extends ProducerTest
{
    /**
     * @test
     *
     * @runInSeparateProcess
     */
    public function sendSyncMessages(): void
    {
        $this->configureProducer();

        $producer = new Producer();
        $messages = $this->createMessages();

        foreach ($messages as $message) {
            $response = $producer->send([$message]);

            self::assertNotEmpty($response);

            foreach ($response[0]['data'][0]['partitions'] as $partition) {
                self::assertSame(0, $partition['errorCode']);
            }
        }
    }
}
