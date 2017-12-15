<?php
namespace Kafka\Consumer;

use DI\FactoryInterface;
use Kafka\Contracts\BrokerInterface;
use Kafka\Contracts\Config\Consumer as ConsumerConfigInterface;
use Kafka\Contracts\Config\Broker as BrokerConfigInterface;
use Kafka\Contracts\Consumer\Process as ProcessInterface;
use Kafka\Contracts\Consumer\State;
use Kafka\Contracts\Consumer\Assignment;
use Psr\Log\LoggerInterface;

class Process implements ProcessInterface
{
    private $broker;

    private $brokerConfig;

    private $consumerConfig;

    protected $consumer = null;

    private $state;

    private $logger;

    private $data;

    private $container;

    private $assign;

    protected $messages = [];

    public function __construct(
        BrokerConfigInterface $brokerConfig,
        ConsumerConfigInterface $consumerConfig,
        BrokerInterface $broker,
        LoggerInterface $logger,
        Data $data,
        FactoryInterface $container,
        Assignment $assign,
        State $state,
        callable $consumer = null
    ) {
        $this->consumerConfig = $consumerConfig;
        $this->broker         = $broker;
        $this->brokerConfig   = $brokerConfig;
        $this->state          = $state;
        $this->consumer       = $consumer;
        $this->logger         = $logger;
        $this->data           = $data;
        $this->container      = $container;
        $this->assign         = $assign;
    }

    /**
     * start consumer
     *
     * @access public
     * @return void
     */
    public function start() : void
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
    public function stop() : void
    {
        // TODO: we should remove the consumer from the group here

        $this->state->stop();
    }

    /**
     * start consumer
     *
     * @access public
     * @return void
     */
    private function init()
    {
        // init protocol
        \Kafka\Protocol::init($this->brokerConfig->getVersion(), $this->logger);

        $this->broker->setProcess(function ($data, $fd) {
            $this->processRequest($data, $fd);
        });

        // init state
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
                    $this->logger->error('Get metadata is fail, brokers or topics is null.');
                    $this->state->failRun(\Kafka\Consumer\State::REQUEST_METADATA);
                } else {
                    $isChange = $this->broker->setData($result['topics'], $result['brokers']);
                    $this->state->succRun(\Kafka\Consumer\State::REQUEST_METADATA, $isChange);
                }
                break;
            case \Kafka\Protocol::GROUP_COORDINATOR_REQUEST:
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::GROUP_COORDINATOR_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] == \Kafka\Protocol::NO_ERROR
                && isset($result['coordinatorId'])) {
                    $this->broker->setGroupBrokerId($result['coordinatorId']);
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
                $this->logger->error('Error request, correlationId:' . $correlationId);
        }
    }

    protected function syncMeta()
    {
        $this->logger->debug('Start sync metadata request');

        $brokerHost = [];
        $brokerList = $this->brokerConfig->getMetadataBrokerList();

        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }

        if (count($brokerHost) == 0) {
            throw new \Kafka\Exception('No valid broker configured');
        }

        shuffle($brokerHost);
        foreach ($brokerHost as $host) {
            $socket = $this->broker->getMetaConnect($host);
            if ($socket) {
                $params = $this->consumerConfig->getTopics();
                $this->logger->debug('Start sync metadata request params:' . json_encode($params));
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
        $connect = $this->broker->getRandConnect();
        if (! $connect) {
            return;
        }
        $params      = [
            'group_id' => $this->consumerConfig->getGroupId(),
        ];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::GROUP_COORDINATOR_REQUEST, $params);
        $connect->write($requestData);
    }

    protected function joinGroup()
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect       = $this->broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return false;
        }
        $memberId    = $this->data->getMemberId();
        $params      = [
            'group_id' => $this->consumerConfig->getGroupId(),
            'session_timeout' => $this->consumerConfig->getSessionTimeout(),
            'rebalance_timeout' => $this->consumerConfig->getRebalanceTimeout(),
            'member_id' => ($memberId == null) ? '' : $memberId,
            'data' => [
                [
                    'protocol_name' => 'range',
                    'version' => 0,
                    'subscription' => $this->consumerConfig->getTopics(),
                    'user_data' => '',
                ],
            ],
        ];
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::JOIN_GROUP_REQUEST, $params);
        $connect->write($requestData);
        $this->logger->debug("Join group start, params:" . json_encode($params));
    }

    public function failJoinGroup($errorCode)
    {
        $memberId = $this->data->getMemberId();
        $error    = sprintf('Join group fail, need rejoin, errorCode %d, memberId: %s', $errorCode, $memberId);
        $this->logger->error($error);
        $this->stateConvert($errorCode);
    }

    public function succJoinGroup($result)
    {
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_JOINGROUP);
        $this->data->setMemberId($result['memberId']);
        $this->data->setGenerationId($result['generationId']);
        if ($result['leaderId'] == $result['memberId']) { // leader assign partition
            $this->assign->assign($this->container, $result['members']);
        }
        $msg = sprintf('Join group sucess, params: %s', json_encode($result));
        $this->logger->debug($msg);
    }

    public function syncGroup()
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect       = $this->broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }
        $memberId     = $this->data->getMemberId();
        $generationId = $this->data->getGenerationId();
        $params       = [
            'group_id' => $this->consumerConfig->getGroupId(),
            'generation_id' => $generationId,
            'member_id' => $memberId,
            'data' => $this->assign->getAssignments(),
        ];
        $requestData  = \Kafka\Protocol::encode(\Kafka\Protocol::SYNC_GROUP_REQUEST, $params);
        $this->logger->debug("Sync group start, params:" . json_encode($params));
        $connect->write($requestData);
    }

    public function failSyncGroup($errorCode)
    {
        $error = sprintf('Sync group fail, need rejoin, errorCode %d', $errorCode);
        $this->logger->error($error);
        $this->stateConvert($errorCode);
    }

    public function succSyncGroup($result)
    {
        $msg = sprintf('Sync group sucess, params: %s', json_encode($result));
        $this->logger->debug($msg);
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_SYNCGROUP);

        $topics         = $this->broker->getTopics();
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
        $this->data->setTopics($brokerToTopics);
    }

    protected function heartbeat()
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect       = $this->broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }
        $memberId = $this->data->getMemberId();
        if (! $memberId) {
            return;
        }
        $generationId = $this->data->getGenerationId();
        $params       = [
            'group_id' => $this->consumerConfig->getGroupId(),
            'generation_id' => $generationId,
            'member_id' => $memberId,
        ];
        $requestData  = \Kafka\Protocol::encode(\Kafka\Protocol::HEART_BEAT_REQUEST, $params);
        //$this->logger->debug("Heartbeat group start, params:" . json_encode($params));
        $connect->write($requestData);
    }

    public function failHeartbeat($errorCode)
    {
        $this->logger->error('Heartbeat error, errorCode:' . $errorCode);
        $this->stateConvert($errorCode);
    }

    protected function offset()
    {
        $context = [];
        $topics  = $this->data->getTopics();
        foreach ($topics as $brokerId => $topicList) {
            $connect = $this->broker->getMetaConnect($brokerId);
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
            //$this->logger->debug("Get current offset start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_REQUEST, $params);
            $connect->write($requestData);
            $context[] = (int) $stream;
        }

        return $context;
    }

    public function succOffset($result, $fd)
    {
        $msg = sprintf('Get current offset sucess, result: %s', json_encode($result));
        //$this->logger->debug($msg);

        $offsets     = $this->data->getOffsets();
        $lastOffsets = $this->data->getLastOffsets();
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
        $this->data->setOffsets($offsets);
        $this->data->setLastOffsets($lastOffsets);
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_OFFSET, $fd);
    }

    protected function fetchOffset()
    {
        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect       = $this->broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }

        $topics = $this->data->getTopics();
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
            'group_id' => $this->consumerConfig->getGroupId(),
            'data' => $data,
        ];
        //$this->logger->debug("Get current fetch offset start, params:" . json_encode($params));
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_FETCH_REQUEST, $params);
        $connect->write($requestData);
    }

    public function succFetchOffset($result)
    {
        $msg = sprintf('Get current fetch offset sucess, result: %s', json_encode($result));
        $this->logger->debug($msg);

        $offsets = $this->data->getFetchOffsets();
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                }

                $offsets[$topic['topicName']][$part['partition']] = $part['offset'];
            }
        }
        $this->data->setFetchOffsets($offsets);

        $consumerOffsets = $this->data->getConsumerOffsets();
        $lastOffsets     = $this->data->getLastOffsets();
        if (empty($consumerOffsets)) {
            $consumerOffsets = $this->data->getFetchOffsets();
            foreach ($consumerOffsets as $topic => $value) {
                foreach ($value as $partId => $offset) {
                    if (isset($lastOffsets[$topic][$partId]) && $lastOffsets[$topic][$partId] > $offset) {
                        $consumerOffsets[$topic][$partId] = $offset + 1;
                    }
                }
            }
            $this->data->setConsumerOffsets($consumerOffsets);
            $this->data->setCommitOffsets($this->data->getFetchOffsets());
        }
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_FETCH_OFFSET);
    }

    protected function fetch()
    {
        $this->messages  = [];
        $context         = [];
        $topics          = $this->data->getTopics();
        $consumerOffsets = $this->data->getConsumerOffsets();
        foreach ($topics as $brokerId => $topicList) {
            $connect = $this->broker->getDataConnect($brokerId);
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
                        'max_bytes' => $this->consumerConfig->getMaxBytes(),
                    ];
                }
                $data[] = $item;
            }
            $params = [
                'max_wait_time' => $this->consumerConfig->getMaxWaitTime(),
                'replica_id' => -1,
                'min_bytes' => '1000',
                'data' => $data,
            ];
            $this->logger->debug("Fetch message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::FETCH_REQUEST, $params);
            $connect->write($requestData);
            $context[] = (int) $connect->getSocket();
        }
        return $context;
    }

    public function succFetch($result, $fd)
    {
        $this->logger->debug('Fetch success, result:' . json_encode($result));
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

                $offset = $this->data->getConsumerOffset($topic['topicName'], $part['partition']);
                if ($offset === false) {
                    return; // current is rejoin....
                }
                foreach ($part['messages'] as $message) {
                    $this->messages[$topic['topicName']][$part['partition']][] = $message;
                    //if ($this->consumer != null) {
                    //    call_user_func($this->consumer, $topic['topicName'], $part['partition'], $message);
                    //}
                    $offset = $message['offset'];
                }

                $consumerOffset = ($part['highwaterMarkOffset'] > $offset) ? ($offset + 1) : $offset;
                $this->data->setConsumerOffset($topic['topicName'], $part['partition'], $consumerOffset);
                $this->data->setCommitOffset($topic['topicName'], $part['partition'], $offset);
            }
        }
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
        if ($this->consumerConfig->getConsumeMode() == \Kafka\Config\Consumer::CONSUME_BEFORE_COMMIT_OFFSET) {
            $this->consumeMessage();
        }

        $groupBrokerId = $this->broker->getGroupBrokerId();
        $connect       = $this->broker->getMetaConnect($groupBrokerId);
        if (! $connect) {
            return;
        }

        $commitOffsets = $this->data->getCommitOffsets();
        $topics        = $this->data->getTopics();
        $this->data->setPrecommitOffsets($commitOffsets);
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
            'group_id' => $this->consumerConfig->getGroupId(),
            'generation_id' => $this->data->getGenerationId(),
            'member_id' => $this->data->getMemberId(),
            'data' => $data,
        ];
        $this->logger->debug("Commit current fetch offset start, params:" . json_encode($params));
        $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_COMMIT_REQUEST, $params);
        $connect->write($requestData);
    }

    public function succCommit($result)
    {
        $this->logger->debug('Commit success, result:' . json_encode($result));
        $this->state->succRun(\Kafka\Consumer\State::REQUEST_COMMIT_OFFSET);
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] != 0) {
                    $this->stateConvert($part['errorCode']);
                    return;  // not call user consumer function
                }
            }
        }
        if ($this->consumerConfig->getConsumeMode() == \Kafka\Config\Consumer::CONSUME_AFTER_COMMIT_OFFSET) {
            $this->consumeMessage();
        }
    }

    protected function stateConvert($errorCode, $context = null)
    {
        $retry = false;
        $this->logger->error(\Kafka\Protocol::getError($errorCode));
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

        if (in_array($errorCode, $recoverCodes)) {
            $this->state->recover();
            $this->data->clearOffset();
            return false;
        }

        if (in_array($errorCode, $rejoinCodes)) {
            if ($errorCode == \Kafka\Protocol::UNKNOWN_MEMBER_ID) {
                $this->data->setMemberId('');
            }
            $this->data->clearOffset();
            $this->state->rejoin();
            return false;
        }

        if (\Kafka\Protocol::OFFSET_OUT_OF_RANGE == $errorCode) {
            $resetOffset = $this->consumerConfig->getOffsetReset();
            if ($resetOffset == 'latest') {
                $offsets = $this->data->getLastOffsets();
            } else {
                $offsets = $this->data->getOffsets();
            }
            list($topic, $partId) = $context;
            if (isset($offsets[$topic][$partId])) {
                $this->data->setConsumerOffset($topic, $partId, $offsets[$topic][$partId]);
            }
        }
        return true;
    }
}
