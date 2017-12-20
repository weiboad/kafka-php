<?php
namespace KafkaTest\Base\Consumer;

use Psr\Log\NullLogger;
use Psr\Log\LoggerInterface;
use \Kafka\Consumer\Process;
use \Kafka\Consumer\State;
use \Kafka\Broker;
use \Kafka\Config\Broker as BrokerConfig;
use \Kafka\Config\Consumer as ConsumerConfig;

class ProcessTest extends \PHPUnit\Framework\TestCase
{
    private $processResponse;

    private $callbacks;

    /**
     * testSyncMeta
     *
     * @access public
     * @return void
     */
    public function testAllCallback()
    {
        $data = $this->createMock(\Kafka\Consumer\Data::class);
        $data->expects($this->exactly(4))
             ->method('getMemberId')
             ->will($this->returnValue('membertest'));
        $data->expects($this->exactly(3))
             ->method('getGenerationId')
             ->will($this->returnValue(22));
        $topics = [
            2 => [
                [
                    'topic_name' => 'test',
                    'partitions' => [0, 1],
                ]
            ]
        ];
        $data->expects($this->exactly(4))
             ->method('getTopics')
             ->will($this->returnValue($topics));
        $data->expects($this->once())
             ->method('getConsumerOffsets')
             ->will($this->returnValue([]));
        $data->expects($this->once())
             ->method('getCommitOffsets')
             ->will($this->returnValue([
                'test' => [0 => 111, 1 => 234]
             ]));
        $data->expects($this->once())
             ->method('setPrecommitOffsets');

        $definition = [
            \Kafka\Consumer\Data::class => function () use ($data) {
                return $data;
            }
        ];

        $container = $this->createProcess($definition, function ($state, $broker) {
            $socketMock = $this->createMock(\Kafka\Contracts\SocketInterface::class);
            $socketMock->method('getSocket')->will($this->returnValue(30));
            $socketMock->expects($this->exactly(9))
                       ->method('write')
                    ->withConsecutive(
                        [$this->equalTo(\hex2bin('0000001d000300000000000300096b61666b612d70687000000001000474657374'))], // syncMeta
                        [$this->equalTo(\hex2bin('00000019000a00000000000a00096b61666b612d706870000474657374'))], // groupId
                        [$this->equalTo(\hex2bin('00000056000b00010000000b00096b61666b612d7068700004746573740000753000007530000a6d656d626572746573740008636f6e73756d657200000001000572616e67650000001000000000000100047465737400000000'))], // joingroup
                        [$this->equalTo(\hex2bin('0000002d000e00000000000e00096b61666b612d70687000047465737400000016000a6d656d6265727465737400000000'))], // syncGroup
                        [$this->equalTo(\hex2bin('00000029000c00000000000c00096b61666b612d70687000047465737400000016000a6d656d62657274657374'))], // heartbeat
                        [$this->equalTo(\hex2bin('0000005f000200000000000200096b61666b612d706870ffffffff000000020004746573740000000100000000ffffffffffffffff000186a00004746573740000000200000000ffffffffffffffff000186a000000001ffffffffffffffff000186a0'))], // offset
                        [$this->equalTo(\hex2bin('0000002f000900010000000900096b61666b612d70687000047465737400000001000474657374000000020000000000000001'))], // fetch offset
                        [$this->equalTo(\hex2bin('0000004d000100020000000100096b61666b612d706870ffffffff00000064000003e800000001000474657374000000020000000000000000000000000001000000000001000000000000000000010000'))], // fetch
                        [$this->equalTo(\hex2bin('0000005b000800020000000800096b61666b612d70687000047465737400000016000a6d656d62657274657374ffffffffffffffff000000010004746573740000000200000000000000000000006f00000000000100000000000000ea0000'))] // commit
                    );
            $broker->expects($this->exactly(7))
                   ->method('getMetaConnect')
                   ->will($this->returnValue($socketMock));
            $broker->expects($this->once())
                   ->method('getRandConnect')
                   ->will($this->returnValue($socketMock));
            $broker->expects($this->once())
                   ->method('getDataConnect')
                   ->will($this->returnValue($socketMock));
            $groupBrokerId = '2';
            $broker->expects($this->exactly(5))
                   ->method('getGroupBrokerId')
                   ->will($this->returnValue($groupBrokerId));
        });
        $brokerConfig = $container->get(\Kafka\Contracts\Config\Broker::class);
        $brokerConfig->setMetadataBrokerList('127.0.0.1:9092');
        $consumerConfig = $container->get(\Kafka\Contracts\Config\Consumer::class);
        $consumerConfig->setTopics(['test']);
        $consumerConfig->setGroupId('test');
        foreach ($this->callbacks as $func) {
            call_user_func($func);
        }
    }

    /**
     * testSyncMetaException
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage It was not possible to establish a connection for metadata with the brokers "127.0.0.1:9092"
     * @access public
     * @return void
     */
    public function testSyncMetaException()
    {
        $container = $this->createProcess([], function ($state, $broker) {
            $broker->expects($this->once())
                   ->method('getMetaConnect')
                   ->will($this->throwException(new \Kafka\Exception()));
        });
        $syncMeta     = $this->callbacks[State::REQUEST_METADATA];
        $brokerConfig = $container->get(\Kafka\Contracts\Config\Broker::class);
        $brokerConfig->setMetadataBrokerList('127.0.0.1:9092');
        $consumerConfig = $container->get(\Kafka\Contracts\Config\Consumer::class);
        $consumerConfig->setTopics(['test']);
        call_user_func($syncMeta);
    }

    /**
     * testStop
     *
     * @access public
     * @return void
     */
    public function testStop()
    {
        $state = $this->createMock(State::class);
        $state->expects($this->once())
              ->method('stop');
        $container = $this->getContainer([
            \Kafka\Contracts\Consumer\State::class => function () use ($state) {
                return $state;
            },
        ]);
        $container->make(\Kafka\Consumer\Process::class)->stop();
    }

    /**
     * testProcessSyncMeta
     *
     * @access public
     * @return void
     */
    public function testProcessSyncMeta()
    {
        $fd   = 2;
        $data = \hex2bin('000000030000000500000009000c31302e37372e39362e313337000023e800000007000b31302e37352e32362e3234000023e800000003000b31302e31332e342e313539000023e800000008000c31302e37372e39362e313336000023e800000004000b31302e31332e342e313630000023e80000000100000004746573740000000a000000000008000000030000000300000003000000070000000800000003000000030000000800000007000000000002000000090000000300000009000000030000000400000003000000040000000900000003000000000005000000070000000300000007000000090000000300000003000000030000000900000007000000000004000000040000000300000004000000070000000800000003000000040000000800000007000000000007000000090000000300000009000000040000000700000003000000040000000900000007000000000001000000080000000300000008000000090000000300000003000000090000000800000003000000000009000000040000000300000004000000080000000900000003000000040000000900000008000000000003000000030000000300000003000000040000000700000003000000030000000400000007000000000006000000080000000300000008000000030000000400000003000000040000000300000008000000000000000000070000000300000007000000080000000900000003000000080000000900000007');

        $result    = json_decode('{"brokers":[{"host":"10.77.96.137","port":9192,"nodeId":9},{"host":"10.75.26.24","port":9192,"nodeId":7},{"host":"10.13.4.159","port":9192,"nodeId":3},{"host":"10.77.96.136","port":9192,"nodeId":8},{"host":"10.13.4.160","port":9192,"nodeId":4}],"topics":[{"topicName":"test","errorCode":0,"partitions":[{"partitionId":8,"errorCode":0,"replicas":[3,7,8],"leader":3,"isr":[3,8,7]},{"partitionId":2,"errorCode":0,"replicas":[9,3,4],"leader":9,"isr":[4,9,3]},{"partitionId":5,"errorCode":0,"replicas":[7,9,3],"leader":7,"isr":[3,9,7]},{"partitionId":4,"errorCode":0,"replicas":[4,7,8],"leader":4,"isr":[4,8,7]},{"partitionId":7,"errorCode":0,"replicas":[9,4,7],"leader":9,"isr":[4,9,7]},{"partitionId":1,"errorCode":0,"replicas":[8,9,3],"leader":8,"isr":[9,8,3]},{"partitionId":9,"errorCode":0,"replicas":[4,8,9],"leader":4,"isr":[4,9,8]},{"partitionId":3,"errorCode":0,"replicas":[3,4,7],"leader":3,"isr":[3,4,7]},{"partitionId":6,"errorCode":0,"replicas":[8,3,4],"leader":8,"isr":[4,3,8]},{"partitionId":0,"errorCode":0,"replicas":[7,8,9],"leader":7,"isr":[8,9,7]}]}]}', true);
        $container = $this->createProcess([], function ($state, $broker) use ($result) {
            $broker->method('setData')
                   ->with($this->equalTo($result['topics']), $this->equalTo($result['brokers']))
                   ->will($this->returnValue(true));
            $state->method('succRun')
                  ->with($this->equalTo(State::REQUEST_METADATA), $this->equalTo(true));
        });
        call_user_func($this->processResponse, $data, $fd);
    }

    /**
     * testProcessGroupBrokerId
     *
     * @access public
     * @return void
     */
    public function testProcessGroupBrokerId()
    {
        $fd     = 2;
        $data   = \hex2bin('0000000a000000000003000b31302e31332e342e313539000023e8');
        $result = json_decode('{"errorCode":0,"coordinatorId":3,"coordinatorHost":"10.13.4.159","coordinatorPort":9192}', true);

        $container = $this->createProcess([], function ($state, $broker) use ($result) {
            $broker->method('setGroupBrokerId')
                   ->with($this->equalTo($result['coordinatorId']));
            $state->method('succRun')
                  ->with($this->equalTo(State::REQUEST_GETGROUP));
        });
        call_user_func($this->processResponse, $data, $fd);

        $data      = \hex2bin('0000000a000100000003000b31302e31332e342e313539000023e8');
        $container = $this->createProcess([], function ($state, $broker) {
            $state->method('failRun')
                  ->with($this->equalTo(State::REQUEST_GETGROUP));
        });
        call_user_func($this->processResponse, $data, $fd);
    }

    /**
     * testProcessJoinGroup
     *
     * @access public
     * @return void
     */
    public function testProcessJoinGroup()
    {
        $fd           = 2;
        $data         = \hex2bin('0000000b000000000001000567726f7570002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d396134393335613934663366002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d39613439333561393466336600000001002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d3961343933356139346633660000001000000000000100047465737400000000');
        $result       = json_decode('{"errorCode":0,"generationId":1,"groupProtocol":"group","leaderId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","members":[{"memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberMeta":{"version":0,"topics":["test"],"userData":""}}]}', true);
        $consumerData = $this->createMock(\Kafka\Consumer\Data::class);
        $assgin       = $this->createMock(\Kafka\Consumer\Assignment::class);
        $definition   = [
            \Kafka\Consumer\Data::class => function () use ($consumerData) {
                return $consumerData;
            },
            \Kafka\Contracts\Consumer\Assignment::class => function () use ($assgin) {
                return $assgin;
            }
        ];
        $consumerData->expects($this->once())
                     ->method('setMemberId')
                     ->with($result['memberId']);
        $consumerData->expects($this->once())
                     ->method('setGenerationId')
                     ->with($result['generationId']);
        $assgin->expects($this->once())
               ->method('assign')
               ->with($this->anything(), $this->equalTo($result['members']));

        $container = $this->createProcess($definition, function ($state, $broker) {
            $state->expects($this->once())
                  ->method('succRun')
                  ->with($this->equalTo(State::REQUEST_JOINGROUP));
        });
        call_user_func($this->processResponse, $data, $fd);
    }

    /**
     * testProcessJoinGroupFail
     *
     * @access public
     * @return void
     */
    public function testProcessJoinGroupFail()
    {
        $fd           = 2;
        $data         = \hex2bin('0000000b000100000001000567726f7570002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d396134393335613934663366002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d39613439333561393466336600000001002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d3961343933356139346633660000001000000000000100047465737400000000');
        $result       = json_decode('{"errorCode":1,"generationId":1,"groupProtocol":"group","leaderId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","members":[{"memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberMeta":{"version":0,"topics":["test"],"userData":""}}]}', true);
        $consumerData = $this->createMock(\Kafka\Consumer\Data::class);
        $definition   = [
            \Kafka\Consumer\Data::class => function () use ($consumerData) {
                return $consumerData;
            },
        ];
        $consumerData->expects($this->once())
                     ->method('getMemberId');
        $container = $this->createProcess($definition);
        call_user_func($this->processResponse, $data, $fd);
    }

    /**
     * testProcessSyncGroup
     *
     * @access public
     * @return void
     */
    public function testProcessSyncGroup()
    {
        $fd     = 2;
        $data   = \hex2bin('0000000e000000000018000000000001000474657374000000010000000000000000');
        $result = json_decode('{"errorCode":0,"partitionAssignments":[{"topicName":"test","partitions":[0]}],"version":0,"userData":""}', true);

        $consumerData = $this->createMock(\Kafka\Consumer\Data::class);
        $definition   = [
            \Kafka\Consumer\Data::class => function () use ($consumerData) {
                return $consumerData;
            },
        ];
        $consumerData->expects($this->once())
                     ->method('setTopics')
                     ->with($this->equalTo(json_decode('{"2":{"test":{"topic_name":"test","partitions":[0]}}}', true)));

        $container = $this->createProcess($definition, function ($state, $broker) {
            $broker->expects($this->once())
                   ->method('getTopics')
                   ->will($this->returnValue(['test' => [0 => 2]]));
            $state->expects($this->once())
                  ->method('succRun')
                  ->with($this->equalTo(State::REQUEST_SYNCGROUP));
        });
        call_user_func($this->processResponse, $data, $fd);
    }

    /**
     * testProcessHeartbeat
     *
     * @access public
     * @return void
     */
    public function testProcessHeartbeat()
    {
        $data      = \hex2bin('0000000c0000');
        $container = $this->createProcess([], function ($state, $broker) {
            $state->expects($this->once())
                  ->method('succRun')
                  ->with($this->equalTo(State::REQUEST_HEARTGROUP));
        });
        call_user_func($this->processResponse, $data, 0);
    }

    /**
     * testProcessOffset
     *
     * @access public
     * @return void
     */
    public function testProcessOffset()
    {
        $fd     = 2;
        $data   = \hex2bin('00000002000000010004746573740000000100000000000000000001000000000000002a');
        $result = json_decode('[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"timestamp":0,"offsets":[42]}]}]', true);

        $consumerData = $this->createMock(\Kafka\Consumer\Data::class);
        $definition   = [
            \Kafka\Consumer\Data::class => function () use ($consumerData) {
                return $consumerData;
            },
        ];
        $consumerData->expects($this->once())
                     ->method('getOffsets');
        $consumerData->expects($this->once())
                     ->method('getLastOffsets');
        $consumerData->expects($this->once())
                     ->method('setLastOffsets')
                     ->with($this->equalTo(['test' => [42]]));
        $consumerData->expects($this->once())
                     ->method('setOffsets')
                     ->with($this->equalTo(['test' => [42]]));

        $container = $this->createProcess($definition, function ($state, $broker) use ($fd) {
            $state->expects($this->once())
                  ->method('succRun')
                  ->with($this->equalTo(State::REQUEST_OFFSET), $this->equalTo($fd));
        });
        call_user_func($this->processResponse, $data, $fd);
    }

    private function createProcess($definition = [], ?callable $mock = null)
    {
        $state = $this->createMock(State::class);
        $state->expects($this->once())
              ->method('start');
        $state->expects($this->once())
              ->method('init');
        $callbacks = [];
        $state->expects($this->once())
               ->method('setCallback')
               ->with($this->arrayHasKey(State::REQUEST_METADATA))
               ->will($this->returnCallback(function ($params) use (&$callbacks) {
                    $callbacks = $params;
               }));
        $processResponse = function () {
        };
        $broker          = $this->createMock(Broker::class);
        $broker->expects($this->once())
               ->method('setProcess')
               ->will($this->returnCallback(function ($params) use (&$processResponse) {
                    $processResponse = $params;
               }));
        if ($mock) {
            call_user_func($mock, $state, $broker);
        }
        $definition = array_merge([
            \Kafka\Contracts\Consumer\State::class => function () use ($state) {
                return $state;
            },
            \Kafka\Contracts\BrokerInterface::class => function () use ($broker) {
                return $broker;
            },
        ], $definition);
        $container = $this->getContainer($definition);
        $container->make(\Kafka\Consumer\Process::class)->start();
        $this->callbacks       = $callbacks;
        $this->processResponse = $processResponse;
        return $container;
    }

    private function getContainer($definition)
    {
        $builder = new \DI\ContainerBuilder();
        $builder->useAnnotations(false);

        $definition = array_merge($this->containerDefinition(), $definition);
        $builder->addDefinitions($definition);
        return $builder->build();
    }

    private function containerDefinition() : array
    {
        return [
            LoggerInterface::class => \DI\object(NullLogger::class),
            \Kafka\Contracts\Consumer\State::class => \DI\object(\Kafka\Consumer\State::class),
            \Kafka\Contracts\Consumer\Assignment::class => \DI\object(\Kafka\Consumer\Assignment::class),
            \Kafka\Contracts\BrokerInterface::class => \DI\object(\Kafka\Broker::class),
            \Kafka\Contracts\SaslMechanism::class => \DI\object(\Kafka\Sasl\NullSasl::class),
            \Kafka\Contracts\SocketInterface::class => \DI\object(\Kafka\Socket\SocketUnblocking::class),
            \Kafka\Contracts\Config\Broker::class => \DI\object(\Kafka\Config\Broker::class),
            \Kafka\Contracts\Config\Socket::class => \DI\object(\Kafka\Config\Socket::class),
            \Kafka\Contracts\Config\Ssl::class => \DI\object(\Kafka\Config\Ssl::class),
            \Kafka\Contracts\Config\Sasl::class => \DI\object(\Kafka\Config\Sasl::class),
            \Kafka\Contracts\Config\Consumer::class => \DI\object(\Kafka\Config\Consumer::class),
        ];
    }
}
