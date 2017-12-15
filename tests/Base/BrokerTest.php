<?php
namespace KafkaTest\Base;

use Kafka\Broker;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class BrokerTest extends \PHPUnit\Framework\TestCase
{

    private $broker;

    public function setUp()
    {
        $builder = new \DI\ContainerBuilder();
        $builder->useAnnotations(false);
        $builder->addDefinitions([
            LoggerInterface::class => \DI\object(NullLogger::class),
            \Kafka\Contracts\BrokerInterface::class => \DI\object(\Kafka\Broker::class),
        ]);
        $container    = $builder->build();
        $this->broker = $container->get(\Kafka\Broker::class);
    }

    /**
     * testGroupBrokerId
     *
     * @access public
     * @return void
     */
    public function testGroupBrokerId()
    {
        $this->broker->setGroupBrokerId(1);
        $this->assertEquals($this->broker->getGroupBrokerId(), 1);
    }

    /**
     * testData
     *
     * @access public
     * @return void
     */
    public function testData()
    {
        $data = [
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
        $this->broker->setData($data['topics'], $data['brokers']);
        $brokers = [
            0 => '127.0.0.1:9092',
            1 => '127.0.0.1:9192',
            2 => '127.0.0.1:9292'
        ];
        $topics  = [
            'test' => [
                0 => 0,
                1 => 2,
            ]
        ];
        $this->assertEquals($brokers, $this->broker->getBrokers());
        $this->assertEquals($topics, $this->broker->getTopics());
    }

    /**
     * testGetConnect
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Invalid broker list, must call in after setData
     * @access public
     * @return void
     */
    public function testConnectRandFalse()
    {
        $this->broker->getRandConnect();
    }

    /**
     * testGetSocket
     *
     * @access public
     * @return void
     */
    public function testGetSocketNotSetConfig()
    {
        $hostname   = '127.0.0.1';
        $port       = '9092';
        $mockSocket = $this->createMock(\Kafka\Socket\SocketBlocking::class);

        $builder = new \DI\ContainerBuilder();
        $builder->useAnnotations(false);
        $builder->addDefinitions([
            LoggerInterface::class => \DI\object(NullLogger::class),
            \Kafka\Contracts\BrokerInterface::class => \DI\object(\Kafka\Broker::class),
            \Kafka\Contracts\SocketInterface::class => function () use ($mockSocket) {
                return $mockSocket;
            }
        ]);
        $brokers   = [
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
        ];
        $container = $builder->build();
        $broker    = $container->get(\Kafka\Broker::class);
        $broker->setData([], $brokers);
        $stream = $broker->getDataConnect($hostname . ':' . $port);
        $this->assertInstanceOf(\Kafka\Contracts\SocketInterface::class, $stream);
    }
}
