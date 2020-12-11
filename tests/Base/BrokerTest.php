<?php
declare(strict_types=1);

namespace KafkaTest\Base;

use Kafka\Broker;
use Kafka\Socket;
use Kafka\SocketFactory;
use Kafka\SocketSync;
use PHPUnit\Framework\TestCase;

class BrokerTest extends TestCase
{
    public function tearDown(): void
    {
        Broker::getInstance()->clear();
    }

    public function testGroupBrokerId(): void
    {
        $broker = Broker::getInstance();
        $broker->setGroupBrokerId(1);

        $this->assertSame($broker->getGroupBrokerId(), 1);
    }

    public function testData(): void
    {
        $broker = $this->getBroker();
        $data   = [
            'brokers' => [
                [
                    'host' => '127.0.0.1',
                    'port' => '9092',
                    'nodeId' => '0',
                ],
                [
                    'host' => '127.0.0.1',
                    'port' => '9192',
                    'nodeId' => '1',
                ],
                [
                    'host' => '127.0.0.1',
                    'port' => '9292',
                    'nodeId' => '2',
                ],
            ],
            'topics' => [
                [
                    'topicName' => 'test',
                    'errorCode' => 0,
                    'partitions' => [
                        [
                            'partitionId' => 0,
                            'errorCode' => 0,
                            'leader' => 0,
                        ],
                        [
                            'partitionId' => 1,
                            'errorCode' => 0,
                            'leader' => 2,
                        ],
                    ],
                ],
                [
                    'topicName' => 'test1',
                    'errorCode' => 25,
                    'partitions' => [
                        [
                            'partitionId' => 0,
                            'errorCode' => 0,
                            'leader' => 0,
                        ],
                        [
                            'partitionId' => 1,
                            'errorCode' => 0,
                            'leader' => 2,
                        ],
                    ],
                ],
            ],
        ];
        $broker->setData($data['topics'], $data['brokers']);
        $brokers = [
            0 => '127.0.0.1:9092',
            1 => '127.0.0.1:9192',
            2 => '127.0.0.1:9292',
        ];
        $topics  = [
            'test' => [
                0 => 0,
                1 => 2,
            ],
        ];
        $this->assertEquals($brokers, $broker->getBrokers());
        $this->assertEquals($topics, $broker->getTopics());
    }

    public function testGetConnect(): void
    {
        $broker = $this->getBroker();
        $data   = [
            [
                'host' => '127.0.0.1',
                'port' => '9092',
                'nodeId' => '0',
            ],
            [
                'host' => '127.0.0.1',
                'port' => '9193',
                'nodeId' => '1',
            ],
            [
                'host' => '127.0.0.1',
                'port' => '9292',
                'nodeId' => '2',
            ],
        ];
        $broker->setData([], $data);

        $socket = $this->getMockBuilder(Socket::class)
            ->setConstructorArgs(['127.0.0.1', '9192'])
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->setMethods(['connect', 'setOnReadable', 'close'])
            ->getMock();

        $result = $broker->getMetaConnect('1');
        $this->assertNull($result);
    }

    public function testGetConnectSyncAndAsyncForTheSameBroker(): void
    {
        $socket = $this->getMockBuilder(Socket::class)
            ->disableOriginalConstructor()
            ->getMock();

        $socketSync = $this->getMockBuilder(SocketSync::class)
            ->disableOriginalConstructor()
            ->getMock();

        $socketFactory = $this->getMockBuilder(SocketFactory::class)
            ->setMethods(['createSocket', 'createSocketSync'])
            ->getMock();

        $socketFactory->method('createSocket')
            ->willReturn($socket);
        $socketFactory->method('createSocketSync')
            ->willReturn($socketSync);

        $broker = $this->getBroker();
        $broker->setSocketFactory($socketFactory);
        $broker->setProcess(function (): void {
        });

        $this->assertSame($socket, $broker->getConnect('kafka:9092', 'metaSockets'));
        $this->assertSame($socketSync, $broker->getConnect('kafka:9092', 'metaSockets', Broker::SOCKET_MODE_SYNC));
    }

    public function testConnectRandFalse(): void
    {
        $broker = $this->getBroker();

        $result = $broker->getRandConnect();
        $this->assertNull($result);
    }

    public function testGetSocketNotSetConfig(): void
    {
        $broker   = $this->getBroker();
        $hostname = '127.0.0.1';
        $port     = 9092;
        $socket   = $broker->getSocket($hostname, $port, Broker::SOCKET_MODE_SYNC);

        $this->assertInstanceOf(SocketSync::class, $socket);
    }

    private function getBroker(): Broker
    {
        return Broker::getInstance();
    }
}
