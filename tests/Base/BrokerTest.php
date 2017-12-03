<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace KafkaTest;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class BrokerTest extends \PHPUnit\Framework\TestCase
{
    // {{{ consts
    // }}}
    // {{{ members
    // }}}
    // {{{ functions
    // {{{ public function setDown()

    /**
     * setDown
     *
     * @access public
     * @return void
     */
    public function tearDown()
    {
        \Kafka\Broker::getInstance()->clear();
    }

    // }}}
    // {{{ public function testGroupBrokerId()

    /**
     * testGroupBrokerId
     *
     * @access public
     * @return void
     */
    public function testGroupBrokerId()
    {
        $broker = \Kafka\Broker::getInstance();
        $broker->setGroupBrokerId(1);
        $this->assertEquals($broker->getGroupBrokerId(), 1);
    }

    // }}}
    // {{{ public function testData()

    /**
     * testData
     *
     * @access public
     * @return void
     */
    public function testData()
    {
        $broker = \Kafka\Broker::getInstance();
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
            2 => '127.0.0.1:9292'
        ];
        $topics  = [
            'test' => [
                0 => 0,
                1 => 2,
            ]
        ];
        $this->assertEquals($brokers, $broker->getBrokers());
        $this->assertEquals($topics, $broker->getTopics());
    }

    // }}}
    // {{{ public function testGetConnect()

    /**
     * testGetConnect
     *
     * @access public
     * @return void
     */
    public function testGetConnect()
    {
        $broker = \Kafka\Broker::getInstance();
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

        $socket = $this->getMockBuilder(\Kafka\Socket::class)
            ->setConstructorArgs(['127.0.0.1', '9192'])
            ->disableOriginalClone()
            ->disableArgumentCloning()
            ->setMethods(['connect', 'setOnReadable', 'close'])
            ->getMock();

        $broker->setSocket($socket);
        $result = $broker->getMetaConnect('1');
        $this->assertFalse($result);

        $broker->setProcess(function ($data) {
        });
        $result = $broker->getMetaConnect('1');
        // second call
        $result = $broker->getMetaConnect('1');
        $this->assertSame($socket, $result);
        $result = $broker->getDataConnect('1');
        $this->assertSame($socket, $result);
        $result = $broker->getDataConnect('127.0.0.1:9292');
        $this->assertSame($socket, $result);
        $result = $broker->getDataConnect('127.0.0.1:9292');
        $this->assertSame($socket, $result);
        $result = $broker->getDataConnect('invalid_key');
        $this->assertFalse($result);
        $result = $broker->getRandConnect();
        $this->assertSame($socket, $result);
        $broker->clear();
    }

    // }}}
    // {{{ public function testConnectRandFalse()

    /**
     * testGetConnect
     *
     * @access public
     * @return void
     */
    public function testConnectRandFalse()
    {
        $broker = \Kafka\Broker::getInstance();

        $result = $broker->getRandConnect();
        $this->assertFalse($result);
    }

    // }}}
    // }}}
}
