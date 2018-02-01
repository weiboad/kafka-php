<?php
namespace Kafka\Consumer;

use Kafka\ConsumerConfig;

class Process
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    protected $consumer = null;

    protected $messages = [];

    public function __construct(callable $consumer = null)
    {
        $this->consumer = $consumer;
    }

    /**
     * start consumer
     *
     * @access public
     * @return void
     */
    public function init()
    {
        // init protocol
        $config = \Kafka\ConsumerConfig::getInstance();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);

        // init process request
        $broker = \Kafka\Broker::getInstance();
        $broker->setConfig($config);
        $broker->setProcess(function ($data, $fd) {
            $this->processRequest($data, $fd);
        });

        // init state
        $this->state = \Kafka\Consumer\State::getInstance();
        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }
        $this->state->setCallback([
            \Kafka\Consumer\State::REQUEST_METADATA => function () {
                return $this->syncMeta();
            },
            \Kafka\Consumer\State::REQUEST_GETGROUP => function () {
                return $this->getGroupBrokerId();
            },
            \Kafka\Consumer\State::REQUEST_JOINGROUP => function () {
                return $this->joinGroup();
            },
            \Kafka\Consumer\State::REQUEST_SYNCGROUP => function () {
                return $this->syncGroup();
            },
            \Kafka\Consumer\State::REQUEST_HEARTGROUP => function () {
                return $this->heartbeat();
            },
            \Kafka\Consumer\State::REQUEST_OFFSET => function () {
                return $this->offset();
            },
            \Kafka\Consumer\State::REQUEST_FETCH_OFFSET => function () {
                return $this->fetchOffset();
            },
            \Kafka\Consumer\State::REQUEST_FETCH => function () {
                return $this->fetch();
            },
            \Kafka\Consumer\State::REQUEST_COMMIT_OFFSET => function () {
                return $this->commit();
            },
        ]);
        $this->state->init();
    }

    /**
     * start consumer
     *
     * @access public
     * @return void
     */
    public function start()
    {
        $this->init();
        $this->state->start();
    }

    /**
     * stop consumer
     *
     * @access public
     * @return void
     */
    public function stop()
    {
        // TODO: we should remove the consumer from the group here

        $this->state->stop();
    }

    /**
     * process Request
     *
     * @access public
     * @return void
     */
    protected function processRequest($data, $fd)
    {
        $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        switch ($correlationId) {
            case \Kafka\Protocol::METADATA_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));
                if (! isset($result['brokers']) || ! isset($result['topics'])) {
                    $this->error('Get metadata is fail, brokers or topics is null.');
                    $this->state->failRun(\Kafka\Consumer\State::REQUEST_METADATA);
                } else {
                    $broker   = \Kafka\Broker::getInstance();
                    $isChange = $broker->setData($result['topics'], $result['brokers']);
                    $this->state->succRun(\Kafka\Consumer\State::REQUEST_METADATA, $isChange);
                }
                break;
            case \Kafka\Protocol::GROUP_COORDINATOR_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::GROUP_COORDINATOR_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] == \Kafka\Protocol::NO_ERROR
                && isset($result['coordinatorId'])) {
                    \Kafka\Broker::getInstance()->setGroupBrokerId($result['coordinatorId']);
                    $this->state->succRun(\Kafka\Consumer\State::REQUEST_GETGROUP);
                } else {
                    $this->state->failRun(\Kafka\Consumer\State::REQUEST_GETGROUP);
                }
                break;
            case \Kafka\Protocol::JOIN_GROUP_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::JOIN_GROUP_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] == 0) {
                    $this->succJoinGroup($result);
                } else {
                    $this->failJoinGroup($result['errorCode']);
                }
                break;
            case \Kafka\Protocol::SYNC_GROUP_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::SYNC_GROUP_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] == 0) {
                    $this->succSyncGroup($result);
                } else {
                    $this->failSyncGroup($result['errorCode']);
                }
                break;
            case \Kafka\Protocol::HEART_BEAT_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::HEART_BEAT_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] == 0) {
                    $this->state->succRun(\Kafka\Consumer\State::REQUEST_HEARTGROUP);
                } else {
                    $this->failHeartbeat($result['errorCode']);
                }
                break;
            case \Kafka\Protocol::OFFSET_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_REQUEST, substr($data, 4));
                $this->succOffset($result, $fd);
                break;
            case \Kafka\Protocol\Protocol::OFFSET_FETCH_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_FETCH_REQUEST, substr($data, 4));
                $this->succFetchOffset($result);
                break;
            case \Kafka\Protocol\Protocol::FETCH_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::FETCH_REQUEST, substr($data, 4));
                $this->succFetch($result, $fd);
                break;
            case \Kafka\Protocol\Protocol::OFFSET_COMMIT_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_COMMIT_REQUEST, substr($data, 4));
                $this->succCommit($result);
                break;
            default:
                $this->error('Error request, correlationId:' . $correlationId);
        }
    }

    protected function syncMeta()
    {
        $this->debug('Start sync metadata request');

        $brokerList = \Kafka\ConsumerConfig::getInstance()->getMetadataBrokerList();
        $brokerHost = [];

        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }

        if (count($brokerHost) == 0) {
            throw new \Kafka\Exception('No valid broker configured');
        }

        shuffle($brokerHost);
        $broker = \Kafka\Broker::getInstance();
        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host);
            if ($socket) {
                $params = \Kafka\ConsumerConfig::getInstance()->getTopics();
                $this->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
                $socket->write($requestData);
                return;
            }
        }

        throw new \Kafka\Exception(
            sprintf(
                'It was not possible to establish a connection for metadata with the brokers "%s"',
                $brokerList
            )
        );
    }

    protected function getGroupBrokerId()
    {
        $broker  = \Kafka\Broker::getInstance();
        $connect = $broker->getRandConnect();
        if (! $connect) {
            return;
        }
        $params      = [
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
        ];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::GROUP_COORDINATOR_REQUEST, $params);
        $connect->write($requestData);
    }

    protected function joinGroup()
    {
        $broker        = \Kafka\Broker::getInstance();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return false;
        }
        $topics      = \Kafka\ConsumerConfig::getInstance()->getTopics();
        $assign      = \Kafka\Consumer\Assignment::getInstance();
        $memberId    = $assign->getMemberId();
        $params      = [
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'session_timeout' => \Kafka\ConsumerConfig::getInstance()->getSessionTimeout(),
            'rebalance_timeout' => \Kafka\ConsumerConfig::getInstance()->getRebalanceTimeout(),
            'member_id' => ($memberId == null) ? '' : $memberId,
            'data' => [
                [
                    'protocol_name' => 'range',
                    'version' => 0,
                    'subscription' => $topics,
                    'user_data' => '',
                ],
            ],
        ];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::JOIN_GROUP_REQUEST, $params);
        $connect->write($requestData);
        $this->debug("Join group start, params:" . json_encode($params));
    }

    public function failJoinGroup($errorCode)
    {
        $assign   = \Kafka\Consumer\Assignment::getInstance();
        $memberId = $assign->getMemberId();
        $error    = sprintf('Join group fail, need rejoin, errorCode %d, memberId: %s', $errorCode, $memberId);
        $this->error($error);
        $this->stateConvert($errorCode);
    }

    public function succJoinGroup($result)
    {
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_JOINGROUP);
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $assign->setMemberId($result['memberId']);
        $assign->setGenerationId($result['generationId']);
        if ($result['leaderId'] == $result['memberId']) { // leader assign partition
            $assigns = $assign->assign($result['members']);
        }
        $msg = sprintf('Join group sucess, params: %s', json_encode($result));
        $this->debug($msg);
    }

    public function syncGroup()
    {
        $broker        = \Kafka\Broker::getInstance();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }
        $topics       = \Kafka\ConsumerConfig::getInstance()->getTopics();
        $assign       = \Kafka\Consumer\Assignment::getInstance();
        $memberId     = $assign->getMemberId();
        $generationId = $assign->getGenerationId();
        $params       = [
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => $generationId,
            'member_id' => $memberId,
            'data' => $assign->getAssignments(),
        ];
        $requestData  = \Kafka\Protocol::encode(\Kafka\Protocol::SYNC_GROUP_REQUEST, $params);
        $this->debug("Sync group start, params:" . json_encode($params));
        $connect->write($requestData);
    }

    public function failSyncGroup($errorCode)
    {
        $error = sprintf('Sync group fail, need rejoin, errorCode %d', $errorCode);
        $this->error($error);
        $this->stateConvert($errorCode);
    }

    public function succSyncGroup($result)
    {
        $msg = sprintf('Sync group sucess, params: %s', json_encode($result));
        $this->debug($msg);
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_SYNCGROUP);

        $topics         = \Kafka\Broker::getInstance()->getTopics();
        $brokerToTopics = [];
        foreach ($result['partitionAssignments'] as $topic) {
            foreach ($topic['partitions'] as $partId) {
                $brokerId = $topics[$topic['topicName']][$partId];
                if (! isset($brokerToTopics[$brokerId])) {
                    $brokerToTopics[$brokerId] = [];
                }

                $topicInfo = [];
                if (isset($brokerToTopics[$brokerId][$topic['topicName']])) {
                    $topicInfo = $brokerToTopics[$brokerId][$topic['topicName']];
                }
                $topicInfo['topic_name'] = $topic['topicName'];
                if (! isset($topicInfo['partitions'])) {
                    $topicInfo['partitions'] = [];
                }
                $topicInfo['partitions'][]                      = $partId;
                $brokerToTopics[$brokerId][$topic['topicName']] = $topicInfo;
            }
        }
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $assign->setTopics($brokerToTopics);
    }

    protected function heartbeat()
    {
        $broker        = \Kafka\Broker::getInstance();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }
        $assign   = \Kafka\Consumer\Assignment::getInstance();
        $memberId = $assign->getMemberId();
        if (! $memberId) {
            return;
        }
        $generationId = $assign->getGenerationId();
        $params       = [
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => $generationId,
            'member_id' => $memberId,
        ];
        $requestData  = \Kafka\Protocol::encode(\Kafka\Protocol::HEART_BEAT_REQUEST, $params);
        //$this->debug("Heartbeat group start, params:" . json_encode($params));
        $connect->write($requestData);
    }

    public function failHeartbeat($errorCode)
    {
        $this->error('Heartbeat error, errorCode:' . $errorCode);
        $this->stateConvert($errorCode);
    }

    protected function offset()
    {
        $context = [];
        $broker  = \Kafka\Broker::getInstance();
        $topics  = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        foreach ($topics as $brokerId => $topicList) {
            $connect = $broker->getMetaConnect($brokerId);
            if (! $connect) {
                return;
            }
            $data = [];
            foreach ($topicList as $topic) {
                $item = [
                    'topic_name' => $topic['topic_name'],
                    'partitions' => [],
                ];
                foreach ($topic['partitions'] as $partId) {
                    $item['partitions'][] = [
                        'partition_id' => $partId,
                        'offset' => 1,
                        'time' =>  -1,
                    ];
                    $data[]               = $item;
                }
            }
            $params = [
                'replica_id' => -1,
                'data' => $data,
            ];
            $stream = $connect->getSocket();
            //$this->debug("Get current offset start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_REQUEST, $params);
            $connect->write($requestData);
            $context[] = (int) $stream;
        }

        return $context;
    }

    public function succOffset($result, $fd)
    {
        $msg = sprintf('Get current offset sucess, result: %s', json_encode($result));
        //$this->debug($msg);

        $offsets     = \Kafka\Consumer\Assignment::getInstance()->getOffsets();
        $lastOffsets = \Kafka\Consumer\Assignment::getInstance()->getLastOffsets();
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                }

                $offsets[$topic['topicName']][$part['partition']]     = end($part['offsets']);
                $lastOffsets[$topic['topicName']][$part['partition']] = $part['offsets'][0];
            }
        }
        \Kafka\Consumer\Assignment::getInstance()->setOffsets($offsets);
        \Kafka\Consumer\Assignment::getInstance()->setLastOffsets($lastOffsets);
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_OFFSET, $fd);
    }

    protected function fetchOffset()
    {
        $broker        = \Kafka\Broker::getInstance();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }

        $topics = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        $data   = [];
        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = [];
                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }
                foreach ($topic['partitions'] as $partId) {
                    $partitions[] = $partId;
                }
                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }
        $params = [
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'data' => $data,
        ];
        //$this->debug("Get current fetch offset start, params:" . json_encode($params));
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_FETCH_REQUEST, $params);
        $connect->write($requestData);
    }

    public function succFetchOffset($result)
    {
        $msg = sprintf('Get current fetch offset sucess, result: %s', json_encode($result));
        $this->debug($msg);

        $assign  = \Kafka\Consumer\Assignment::getInstance();
        $offsets = $assign->getFetchOffsets();
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                }

                $offsets[$topic['topicName']][$part['partition']] = $part['offset'];
            }
        }
        $assign->setFetchOffsets($offsets);

        $consumerOffsets = $assign->getConsumerOffsets();
        $lastOffsets     = $assign->getLastOffsets();
        if (empty($consumerOffsets)) {
            $consumerOffsets = $assign->getFetchOffsets();
            foreach ($consumerOffsets as $topic => $value) {
                foreach ($value as $partId => $offset) {
                    if (isset($lastOffsets[$topic][$partId]) && $lastOffsets[$topic][$partId] > $offset) {
                        $consumerOffsets[$topic][$partId] = $offset + 1;
                    }
                }
            }
            $assign->setConsumerOffsets($consumerOffsets);
            $assign->setCommitOffsets($assign->getFetchOffsets());
        }
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_FETCH_OFFSET);
    }

    protected function fetch()
    {
        $this->messages  = [];
        $context         = [];
        $broker          = \Kafka\Broker::getInstance();
        $topics          = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        $consumerOffsets = \Kafka\Consumer\Assignment::getInstance()->getConsumerOffsets();
        foreach ($topics as $brokerId => $topicList) {
            $connect = $broker->getDataConnect($brokerId);
            if (! $connect) {
                return;
            }

            $data = [];
            foreach ($topicList as $topic) {
                $item = [
                    'topic_name' => $topic['topic_name'],
                    'partitions' => [],
                ];
                foreach ($topic['partitions'] as $partId) {
                    $item['partitions'][] = [
                        'partition_id' => $partId,
                        'offset' => isset($consumerOffsets[$topic['topic_name']][$partId]) ? $consumerOffsets[$topic['topic_name']][$partId] : 0,
                        'max_bytes' => \Kafka\ConsumerConfig::getInstance()->getMaxBytes(),
                    ];
                }
                $data[] = $item;
            }
            $params = [
                'max_wait_time' => \Kafka\ConsumerConfig::getInstance()->getMaxWaitTime(),
                'replica_id' => -1,
                'min_bytes' => '1000',
                'data' => $data,
            ];
            $this->debug("Fetch message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::FETCH_REQUEST, $params);
            $connect->write($requestData);
            $context[] = (int) $connect->getSocket();
        }
        return $context;
    }

    public function succFetch($result, $fd)
    {
        $assign = \Kafka\Consumer\Assignment::getInstance();
        $this->debug('Fetch success, result:' . json_encode($result));
        foreach ($result['topics'] as $topic) {
            foreach ($topic['partitions'] as $part) {
                $context = [
                    $topic['topicName'],
                    $part['partition'],
                ];
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode'], $context);
                    continue;
                }

                $consumerOffset = $assign->getConsumerOffset($topic['topicName'], $part['partition']);
                if ($consumerOffset === false) {
                    return; // current is rejoin....
                }
                foreach ($part['messages'] as $message) {
                    $this->messages[$topic['topicName']][$part['partition']][] = $message;
                    //if ($this->consumer != null) {
                    //    call_user_func($this->consumer, $topic['topicName'], $part['partition'], $message);
                    //}
                    $commitOffset = $message['offset'];
                }

                $commitOffset = isset($commitOffset) ? $commitOffset : $consumerOffset - 1;
                $consumerOffset = $commitOffset + 1;

                $assign->setConsumerOffset($topic['topicName'], $part['partition'], $consumerOffset);
                $assign->setCommitOffset($topic['topicName'], $part['partition'], $commitOffset);
            }
        }
        //获取消息中没有消息时，不能提交才行
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_FETCH, $fd);
    }

    protected function consumeMessage()
    {
        foreach ($this->messages as $topic => $value) {
            foreach ($value as $part => $messages) {
                foreach ($messages as $message) {
                    if ($this->consumer != null) {
                        call_user_func($this->consumer, $topic, $part, $message);
                    }
                }
            }
        }

        $this->messages = [];
    }

    protected function commit()
    {
        $config = ConsumerConfig::getInstance();
        if ($config->getConsumeMode() == ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET) {
            $this->consumeMessage();
        }

        $broker        = \Kafka\Broker::getInstance();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }

        $commitOffsets = \Kafka\Consumer\Assignment::getInstance()->getCommitOffsets();
        $topics        = \Kafka\Consumer\Assignment::getInstance()->getTopics();
        \Kafka\Consumer\Assignment::getInstance()->setPrecommitOffsets($commitOffsets);
        $data = [];
        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = [];
                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }
                foreach ($topic['partitions'] as $partId) {
                    if ($commitOffsets[$topic['topic_name']][$partId] == -1) {
                        continue;
                    }
                    $partitions[$partId]['partition'] = $partId;
                    $partitions[$partId]['offset']    = $commitOffsets[$topic['topic_name']][$partId];
                }
                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }
        $params = [
            'group_id' => \Kafka\ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => \Kafka\Consumer\Assignment::getInstance()->getGenerationId(),
            'member_id' => \Kafka\Consumer\Assignment::getInstance()->getMemberId(),
            'data' => $data,
        ];
        $this->debug("Commit current fetch offset start, params:" . json_encode($params));
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_COMMIT_REQUEST, $params);
        $connect->write($requestData);
    }

    /**
     * @var State
     */
    public $state;
    public function succCommit($result)
    {
        $this->debug('Commit success, result:' . json_encode($result));
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_COMMIT_OFFSET);
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode']);
                    return;  // not call user consumer function
                }
            }
        }
        if (ConsumerConfig::getInstance()->getConsumeMode() == ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET) {
            $this->consumeMessage();
        }
    }

    protected function stateConvert($errorCode, $context = null)
    {
        $retry = false;
        $this->error(\Kafka\Protocol::getError($errorCode));
        $recoverCodes = [
            \Kafka\Protocol::UNKNOWN_TOPIC_OR_PARTITION,
            \Kafka\Protocol::NOT_LEADER_FOR_PARTITION,
            \Kafka\Protocol::BROKER_NOT_AVAILABLE,
            \Kafka\Protocol::GROUP_LOAD_IN_PROGRESS,
            \Kafka\Protocol::GROUP_COORDINATOR_NOT_AVAILABLE,
            \Kafka\Protocol::NOT_COORDINATOR_FOR_GROUP,
            \Kafka\Protocol::INVALID_TOPIC,
            \Kafka\Protocol::INCONSISTENT_GROUP_PROTOCOL,
            \Kafka\Protocol::INVALID_GROUP_ID,
        ];
        $rejoinCodes  = [
            \Kafka\Protocol::ILLEGAL_GENERATION,
            \Kafka\Protocol::INVALID_SESSION_TIMEOUT,
            \Kafka\Protocol::REBALANCE_IN_PROGRESS,
            \Kafka\Protocol::UNKNOWN_MEMBER_ID,
        ];

        $assign = \Kafka\Consumer\Assignment::getInstance();
        if (in_array($errorCode, $recoverCodes)) {
            $this->state->recover();
            $assign->clearOffset();
            return false;
        }

        if (in_array($errorCode, $rejoinCodes)) {
            if ($errorCode == \Kafka\Protocol::UNKNOWN_MEMBER_ID) {
                $assign->setMemberId('');
            }
            $assign->clearOffset();
            $this->state->rejoin();
            return false;
        }

        if (\Kafka\Protocol::OFFSET_OUT_OF_RANGE == $errorCode) {
            $resetOffset = \Kafka\ConsumerConfig::getInstance()->getOffsetReset();
            if ($resetOffset == 'latest') {
                $offsets = $assign->getLastOffsets();
            } else {
                $offsets = $assign->getOffsets();
            }
            list($topic, $partId) = $context;
            if (isset($offsets[$topic][$partId])) {
                $assign->setConsumerOffset($topic, $partId, $offsets[$topic][$partId]);
            }
        }
        return true;
    }
}
