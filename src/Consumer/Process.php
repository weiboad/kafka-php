<?php
declare(strict_types=1);

namespace Kafka\Consumer;

use Kafka\Broker;
use Kafka\ConsumerConfig;
use Kafka\Exception;
use Kafka\LoggerTrait;
use Kafka\Protocol;
use Kafka\Protocol\Protocol as ProtocolTool;
use Psr\Log\LoggerAwareTrait;

class Process
{
    use LoggerAwareTrait;
    use LoggerTrait;

    /**
     * @var callable|null
     */
    protected $consumer;

    /**
     * @var string[][][]
     */
    protected $messages = [];

    /**
     * @var State
     */
    private $state;

    public function __construct(?callable $consumer = null)
    {
        $this->consumer = $consumer;
    }

    public function init(): void
    {
        $config = $this->getConfig();
        Protocol::init($config->getBrokerVersion(), $this->logger);

        $broker = $this->getBroker();
        $broker->setConfig($config);
        $broker->setProcess(function (string $data, int $fd): void {
            $this->processRequest($data, $fd);
        });

        $this->state = State::getInstance();

        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }
        $this->state->setCallback(
            [
                State::REQUEST_METADATA      => function (): void {
                    $this->syncMeta();
                },
                State::REQUEST_GETGROUP      => function (): void {
                    $this->getGroupBrokerId();
                },
                State::REQUEST_JOINGROUP     => function (): void {
                    $this->joinGroup();
                },
                State::REQUEST_SYNCGROUP     => function (): void {
                    $this->syncGroup();
                },
                State::REQUEST_HEARTGROUP    => function (): void {
                    $this->heartbeat();
                },
                State::REQUEST_OFFSET        => function (): array {
                    return $this->offset();
                },
                State::REQUEST_FETCH_OFFSET  => function (): void {
                    $this->fetchOffset();
                },
                State::REQUEST_FETCH         => function (): array {
                    return $this->fetch();
                },
                State::REQUEST_COMMIT_OFFSET => function (): void {
                    $this->commit();
                },
            ]
        );
        $this->state->init();
    }

    public function start(): void
    {
        $this->init();
        $this->state->start();
    }

    public function stop(): void
    {
        // TODO: we should remove the consumer from the group here

        $this->state->stop();
    }

    /**
     * @throws Exception
     */
    protected function processRequest(string $data, int $fd): void
    {
        $correlationId = ProtocolTool::unpack(ProtocolTool::BIT_B32, \substr($data, 0, 4));

        switch ($correlationId) {
            case Protocol::METADATA_REQUEST:
                $result = Protocol::decode(Protocol::METADATA_REQUEST, \substr($data, 4));

                if (! isset($result['brokers'], $result['topics'])) {
                    $this->error('Get metadata is fail, brokers or topics is null.');
                    $this->state->failRun(State::REQUEST_METADATA);
                    break;
                }

                /** @var Broker $broker */
                $broker   = $this->getBroker();
                $isChange = $broker->setData($result['topics'], $result['brokers']);
                $this->state->succRun(State::REQUEST_METADATA, $isChange);

                break;
            case Protocol::GROUP_COORDINATOR_REQUEST:
                $result = Protocol::decode(Protocol::GROUP_COORDINATOR_REQUEST, \substr($data, 4));

                if (! isset($result['errorCode'], $result['coordinatorId']) || $result['errorCode'] !== Protocol::NO_ERROR) {
                    $this->state->failRun(State::REQUEST_GETGROUP);
                    break;
                }

                /** @var Broker $broker */
                $broker = $this->getBroker();
                $broker->setGroupBrokerId($result['coordinatorId']);

                $this->state->succRun(State::REQUEST_GETGROUP);

                break;
            case Protocol::JOIN_GROUP_REQUEST:
                $result = Protocol::decode(Protocol::JOIN_GROUP_REQUEST, \substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->succJoinGroup($result);
                    break;
                }

                $this->failJoinGroup($result['errorCode']);
                break;
            case Protocol::SYNC_GROUP_REQUEST:
                $result = Protocol::decode(Protocol::SYNC_GROUP_REQUEST, \substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->succSyncGroup($result);
                    break;
                }

                $this->failSyncGroup($result['errorCode']);
                break;
            case Protocol::HEART_BEAT_REQUEST:
                $result = Protocol::decode(Protocol::HEART_BEAT_REQUEST, \substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->state->succRun(State::REQUEST_HEARTGROUP);
                    break;
                }

                $this->failHeartbeat($result['errorCode']);
                break;
            case Protocol::OFFSET_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_REQUEST, \substr($data, 4));
                $this->succOffset($result, $fd);
                break;
            case ProtocolTool::OFFSET_FETCH_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_FETCH_REQUEST, \substr($data, 4));
                $this->succFetchOffset($result);
                break;
            case ProtocolTool::FETCH_REQUEST:
                $result = Protocol::decode(Protocol::FETCH_REQUEST, \substr($data, 4));
                $this->succFetch($result, $fd);
                break;
            case ProtocolTool::OFFSET_COMMIT_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_COMMIT_REQUEST, \substr($data, 4));
                $this->succCommit($result);
                break;
            default:
                $this->error('Error request, correlationId:' . $correlationId);
        }
    }

    protected function syncMeta(): void
    {
        $this->debug('Start sync metadata request');

        $config = $this->getConfig();

        $brokerList = $config->getMetadataBrokerList();
        $brokerHost = [];

        foreach (\explode(',', $brokerList) as $key => $val) {
            if (\trim($val)) {
                $brokerHost[] = $val;
            }
        }

        if (\count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }

        \shuffle($brokerHost);
        $broker = $this->getBroker();

        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host);

            if ($socket === null) {
                continue;
            }

            $params = $config->getTopics();
            $this->debug('Start sync metadata request params:' . \json_encode($params));
            $requestData = Protocol::encode(Protocol::METADATA_REQUEST, $params);
            $socket->write($requestData);

            return;
        }

        throw new Exception(
            \sprintf('It was not possible to establish a connection for metadata with the brokers "%s"', $brokerList)
        );
    }

    protected function getGroupBrokerId(): void
    {
        $broker  = $this->getBroker();
        $connect = $broker->getRandConnect();

        if ($connect === null) {
            return;
        }

        $config = $this->getConfig();
        $params = ['group_id' => $config->getGroupId()];

        $requestData = Protocol::encode(Protocol::GROUP_COORDINATOR_REQUEST, $params);
        $connect->write($requestData);
    }

    protected function joinGroup(): void
    {
        $broker = $this->getBroker();

        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return;
        }

        $topics   = $this->getConfig()->getTopics();
        $assign   = $this->getAssignment();
        $memberId = $assign->getMemberId();

        $params = [
            'group_id'          => $this->getConfig()->getGroupId(),
            'session_timeout'   => $this->getConfig()->getSessionTimeout(),
            'rebalance_timeout' => $this->getConfig()->getRebalanceTimeout(),
            'member_id'         => $memberId ?? '',
            'data'              => [
                [
                    'protocol_name' => 'range',
                    'version'       => 0,
                    'subscription'  => $topics,
                    'user_data'     => '',
                ],
            ],
        ];

        $requestData = Protocol::encode(Protocol::JOIN_GROUP_REQUEST, $params);
        $connect->write($requestData);
        $this->debug('Join group start, params:' . \json_encode($params));
    }

    public function failJoinGroup(int $errorCode): void
    {
        $assign   = $this->getAssignment();
        $memberId = $assign->getMemberId();

        $this->error(\sprintf('Join group fail, need rejoin, errorCode %d, memberId: %s', $errorCode, $memberId));
        $this->stateConvert($errorCode);
    }

    /**
     * @param mixed[] $result
     */
    public function succJoinGroup(array $result): void
    {
        $this->state->succRun(State::REQUEST_JOINGROUP);
        $assign = $this->getAssignment();
        $assign->setMemberId($result['memberId']);
        $assign->setGenerationId($result['generationId']);

        if ($result['leaderId'] === $result['memberId']) { // leader assign partition
            $assign->assign($result['members']);
        }

        $this->debug(\sprintf('Join group sucess, params: %s', \json_encode($result)));
    }

    public function syncGroup(): void
    {
        $broker        = $this->getBroker();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return;
        }

        $assign       = $this->getAssignment();
        $memberId     = $assign->getMemberId();
        $generationId = $assign->getGenerationId();

        $params = [
            'group_id'      => $this->getConfig()->getGroupId(),
            'generation_id' => $generationId ?? null,
            'member_id'     => $memberId,
            'data'          => $assign->getAssignments(),
        ];

        $requestData = Protocol::encode(Protocol::SYNC_GROUP_REQUEST, $params);
        $this->debug('Sync group start, params:' . \json_encode($params));

        $connect->write($requestData);
    }

    public function failSyncGroup(int $errorCode): void
    {
        $this->error(\sprintf('Sync group fail, need rejoin, errorCode %d', $errorCode));
        $this->stateConvert($errorCode);
    }

    /**
     * @param mixed[][] $result
     */
    public function succSyncGroup(array $result): void
    {
        $this->debug(\sprintf('Sync group sucess, params: %s', \json_encode($result)));
        $this->state->succRun(State::REQUEST_SYNCGROUP);

        $topics = $this->getBroker()->getTopics();

        $brokerToTopics = [];

        foreach ($result['partitionAssignments'] as $topic) {
            foreach ($topic['partitions'] as $partId) {
                $brokerId = $topics[$topic['topicName']][$partId];

                $brokerToTopics[$brokerId] = $brokerToTopics[$brokerId] ?? [];

                $topicInfo = $brokerToTopics[$brokerId][$topic['topicName']] ?? [];

                $topicInfo['topic_name'] = $topic['topicName'];

                $topicInfo['partitions']   = $topicInfo['partitions'] ?? [];
                $topicInfo['partitions'][] = $partId;

                $brokerToTopics[$brokerId][$topic['topicName']] = $topicInfo;
            }
        }

        $assign = $this->getAssignment();
        $assign->setTopics($brokerToTopics);
    }

    protected function heartbeat(): void
    {
        $broker        = $this->getBroker();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return;
        }

        $assign   = $this->getAssignment();
        $memberId = $assign->getMemberId();

        if (\trim($memberId) === '') {
            return;
        }

        $generationId = $assign->getGenerationId();

        $params = [
            'group_id'      => $this->getConfig()->getGroupId(),
            'generation_id' => $generationId,
            'member_id'     => $memberId,
        ];

        $requestData = Protocol::encode(Protocol::HEART_BEAT_REQUEST, $params);
        $connect->write($requestData);
    }

    public function failHeartbeat(int $errorCode): void
    {
        $this->error('Heartbeat error, errorCode:' . $errorCode);
        $this->stateConvert($errorCode);
    }

    /**
     * @return int[]
     */
    protected function offset(): array
    {
        $context = [];
        $broker  = $this->getBroker();
        $topics  = $this->getAssignment()->getTopics();

        foreach ($topics as $brokerId => $topicList) {
            $connect = $broker->getMetaConnect((string) $brokerId);

            if ($connect === null) {
                return [];
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
                'data'       => $data,
            ];

            $stream      = $connect->getSocket();
            $requestData = Protocol::encode(Protocol::OFFSET_REQUEST, $params);

            $connect->write($requestData);
            $context[] = (int) $stream;
        }

        return $context;
    }

    /**
     * @param mixed[][] $result
     */
    public function succOffset(array $result, int $fd): void
    {
        $offsets     = $this->getAssignment()->getOffsets();
        $lastOffsets = $this->getAssignment()->getLastOffsets();

        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== Protocol::NO_ERROR) {
                    $this->stateConvert($part['errorCode']);
                    break 2;
                }

                $offsets[$topic['topicName']][$part['partition']]     = \end($part['offsets']);
                $lastOffsets[$topic['topicName']][$part['partition']] = $part['offsets'][0];
            }
        }

        $this->getAssignment()->setOffsets($offsets);
        $this->getAssignment()->setLastOffsets($lastOffsets);
        $this->state->succRun(State::REQUEST_OFFSET, $fd);
    }

    protected function fetchOffset(): void
    {
        $broker        = $this->getBroker();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return;
        }

        $topics = $this->getAssignment()->getTopics();
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
            'group_id' => $this->getConfig()->getGroupId(),
            'data'     => $data,
        ];

        $requestData = Protocol::encode(Protocol::OFFSET_FETCH_REQUEST, $params);
        $connect->write($requestData);
    }

    /**
     * @param mixed[] $result
     */
    public function succFetchOffset(array $result): void
    {
        $msg = \sprintf('Get current fetch offset sucess, result: %s', \json_encode($result));
        $this->debug($msg);

        $assign  = $this->getAssignment();
        $offsets = $assign->getFetchOffsets();

        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== 0) {
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
                        $consumerOffsets[$topic][$partId] = $offset;
                    }
                }
            }

            $assign->setConsumerOffsets($consumerOffsets);
            $assign->setCommitOffsets($assign->getFetchOffsets());
        }

        $this->state->succRun(State::REQUEST_FETCH_OFFSET);
    }

    /**
     * @return int[]
     */
    protected function fetch(): array
    {
        $this->messages  = [];
        $context         = [];
        $broker          = $this->getBroker();
        $topics          = $this->getAssignment()->getTopics();
        $consumerOffsets = $this->getAssignment()->getConsumerOffsets();

        foreach ($topics as $brokerId => $topicList) {
            $connect = $broker->getDataConnect((string) $brokerId);

            if ($connect === null) {
                return [];
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
                        'max_bytes' => $this->getConfig()->getMaxBytes(),
                    ];
                }

                $data[] = $item;
            }

            $params = [
                'max_wait_time' => $this->getConfig()->getMaxWaitTime(),
                'replica_id' => -1,
                'min_bytes' => '1000',
                'data' => $data,
            ];

            $this->debug('Fetch message start, params:' . \json_encode($params));
            $requestData = Protocol::encode(Protocol::FETCH_REQUEST, $params);
            $connect->write($requestData);
            $context[] = (int) $connect->getSocket();
        }

        return $context;
    }

    /**
     * @param mixed[][][] $result
     */
    public function succFetch(array $result, int $fd): void
    {
        $assign = $this->getAssignment();
        $this->debug('Fetch success, result:' . \json_encode($result));

        foreach ($result['topics'] as $topic) {
            foreach ($topic['partitions'] as $part) {
                $context = [
                    $topic['topicName'],
                    $part['partition'],
                ];

                if ($part['errorCode'] !== 0) {
                    $this->stateConvert($part['errorCode'], $context);
                    continue;
                }

                $offset = $assign->getConsumerOffset($topic['topicName'], $part['partition']);

                if ($offset === null) {
                    return; // current is rejoin....
                }

                foreach ($part['messages'] as $message) {
                    $this->messages[$topic['topicName']][$part['partition']][] = $message;

                    $offset = $message['offset'];
                }

                $consumerOffset = ($offset + 1);
                $assign->setConsumerOffset($topic['topicName'], $part['partition'], $consumerOffset);
                $assign->setCommitOffset($topic['topicName'], $part['partition'], $offset);
            }
        }

        $this->state->succRun(State::REQUEST_FETCH, $fd);
    }

    protected function consumeMessage(): void
    {
        foreach ($this->messages as $topic => $value) {
            foreach ($value as $partition => $messages) {
                foreach ($messages as $message) {
                    if ($this->consumer !== null) {
                        ($this->consumer)($topic, $partition, $message);
                    }
                }
            }
        }

        $this->messages = [];
    }

    protected function commit(): void
    {
        $config = $this->getConfig();

        if ($config->getConsumeMode() === ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET) {
            $this->consumeMessage();
        }

        $broker        = $this->getBroker();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return;
        }

        $commitOffsets = $this->getAssignment()->getCommitOffsets();
        $topics        = $this->getAssignment()->getTopics();
        $this->getAssignment()->setPreCommitOffsets($commitOffsets);
        $data = [];

        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = [];

                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }

                foreach ($topic['partitions'] as $partId) {
                    if ($commitOffsets[$topic['topic_name']][$partId] === -1) {
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
            'group_id' => $this->getConfig()->getGroupId(),
            'generation_id' => $this->getAssignment()->getGenerationId(),
            'member_id' => $this->getAssignment()->getMemberId(),
            'data' => $data,
        ];

        $this->debug('Commit current fetch offset start, params:' . \json_encode($params));
        $requestData = Protocol::encode(Protocol::OFFSET_COMMIT_REQUEST, $params);
        $connect->write($requestData);
    }

    /**
     * @param mixed[][] $result
     */
    public function succCommit(array $result): void
    {
        $this->debug('Commit success, result:' . \json_encode($result));
        $this->state->succRun(State::REQUEST_COMMIT_OFFSET);

        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== 0) {
                    $this->stateConvert($part['errorCode']);
                    return;  // not call user consumer function
                }
            }
        }

        if ($this->getConfig()->getConsumeMode() === ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET) {
            $this->consumeMessage();
        }
    }

    /**
     * @param string[] $context
     */
    protected function stateConvert(int $errorCode, ?array $context = null): bool
    {
        $this->error(Protocol::getError($errorCode));

        $recoverCodes = [
            Protocol::UNKNOWN_TOPIC_OR_PARTITION,
            Protocol::NOT_LEADER_FOR_PARTITION,
            Protocol::BROKER_NOT_AVAILABLE,
            Protocol::GROUP_LOAD_IN_PROGRESS,
            Protocol::GROUP_COORDINATOR_NOT_AVAILABLE,
            Protocol::NOT_COORDINATOR_FOR_GROUP,
            Protocol::INVALID_TOPIC,
            Protocol::INCONSISTENT_GROUP_PROTOCOL,
            Protocol::INVALID_GROUP_ID,
        ];

        $rejoinCodes = [
            Protocol::ILLEGAL_GENERATION,
            Protocol::INVALID_SESSION_TIMEOUT,
            Protocol::REBALANCE_IN_PROGRESS,
            Protocol::UNKNOWN_MEMBER_ID,
        ];

        $assign = $this->getAssignment();

        if (\in_array($errorCode, $recoverCodes, true)) {
            $this->state->recover();
            $assign->clearOffset();
            return false;
        }

        if (\in_array($errorCode, $rejoinCodes, true)) {
            if ($errorCode === Protocol::UNKNOWN_MEMBER_ID) {
                $assign->setMemberId('');
            }

            $assign->clearOffset();
            $this->state->rejoin();
            return false;
        }

        if ($errorCode === Protocol::OFFSET_OUT_OF_RANGE) {
            $resetOffset = $this->getConfig()->getOffsetReset();
            $offsets     = $resetOffset === 'latest' ? $assign->getLastOffsets() : $assign->getOffsets();

            [$topic, $partId] = $context;

            if (isset($offsets[$topic][$partId])) {
                $assign->setConsumerOffset($topic, (int) $partId, $offsets[$topic][$partId]);
            }
        }

        return true;
    }

    private function getBroker(): Broker
    {
        return Broker::getInstance();
    }

    private function getConfig(): ConsumerConfig
    {
        return ConsumerConfig::getInstance();
    }

    private function getAssignment(): Assignment
    {
        return Assignment::getInstance();
    }
}
