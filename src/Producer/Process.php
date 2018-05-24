<?php
declare(strict_types=1);

namespace Kafka\Producer;

use Amp\Loop;
use Kafka\Broker;
use Kafka\Exception;
use Kafka\LoggerTrait;
use Kafka\ProducerConfig;
use Kafka\Protocol;
use Psr\Log\LoggerAwareTrait;
use function count;
use function explode;
use function in_array;
use function json_encode;
use function shuffle;
use function sprintf;
use function substr;
use function trim;

class Process
{
    use LoggerAwareTrait;
    use LoggerTrait;

    /**
     * @var callable|null
     */
    protected $producer;

    /**
     * @var bool
     */
    protected $isRunning = true;

    /**
     * @var callable|null
     */
    protected $success;

    /**
     * @var callable|null
     */
    protected $error;

    /**
     * @var State
     */
    private $state;

    /** @var RecordValidator */
    private $recordValidator;

    public function __construct(?callable $producer = null, ?RecordValidator $recordValidator = null)
    {
        $this->producer        = $producer;
        $this->recordValidator = $recordValidator ?? new RecordValidator();
    }

    public function init(): void
    {
        $config = $this->getConfig();
        Protocol::init($config->getBrokerVersion(), $this->logger);

        $broker = $this->getBroker();
        $broker->setConfig($config);
        $broker->setProcess(
            function (string $data, int $fd): void {
                $this->processRequest($data, $fd);
            }
        );

        $this->state = State::getInstance();

        if ($this->logger) {
            $this->state->setLogger($this->logger);
        }

        $this->state->setCallback(
            [
                State::REQUEST_METADATA => [$this, 'syncMeta'],
                State::REQUEST_PRODUCE  => [$this, 'produce'],
            ]
        );

        $this->state->init();

        if (! empty($broker->getTopics())) {
            $this->state->succRun(State::REQUEST_METADATA);
        }
    }

    public function start(): void
    {
        $this->init();
        $this->state->start();

        $config = $this->getConfig();

        if ($config->getIsAsyn()) {
            return;
        }

        Loop::repeat(
            $config->getRequestTimeout(),
            function (string $watcherId): void {
                if ($this->error !== null) {
                    ($this->error)(1000);
                }

                Loop::cancel($watcherId);
                Loop::stop();
            }
        );
    }

    public function stop(): void
    {
        $this->isRunning = false;
    }

    public function setSuccess(callable $success): void
    {
        $this->success = $success;
    }

    public function setError(callable $error): void
    {
        $this->error = $error;
    }

    /**
     * @throws \Kafka\Exception
     */
    public function syncMeta(): void
    {
        $this->debug('Start sync metadata request');

        $brokerList = ProducerConfig::getInstance()->getMetadataBrokerList();
        $brokerHost = [];

        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }

        if (count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }

        shuffle($brokerHost);
        $broker = $this->getBroker();

        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host);

            if ($socket !== null) {
                $params = [];
                $this->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = Protocol::encode(Protocol::METADATA_REQUEST, $params);
                $socket->write($requestData);

                return;
            }
        }

        throw Exception\ConnectionException::fromBrokerList($brokerList);
    }

    /**
     * process Request
     *
     * @throws \Kafka\Exception
     */
    protected function processRequest(string $data, int $fd): void
    {
        $correlationId = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        switch ($correlationId) {
            case Protocol::METADATA_REQUEST:
                $result = Protocol::decode(Protocol::METADATA_REQUEST, substr($data, 4));

                if (! isset($result['brokers'], $result['topics'])) {
                    $this->error('Get metadata is fail, brokers or topics is null.');
                    $this->state->failRun(State::REQUEST_METADATA);
                    break;
                }

                $broker   = $this->getBroker();
                $isChange = $broker->setData($result['topics'], $result['brokers']);
                $this->state->succRun(State::REQUEST_METADATA, $isChange);

                break;
            case Protocol::PRODUCE_REQUEST:
                $result = Protocol::decode(Protocol::PRODUCE_REQUEST, substr($data, 4));
                $this->succProduce($result, $fd);
                break;
            default:
                $this->error('Error request, correlationId:' . $correlationId);
        }
    }

    /**
     * @return int[]
     */
    public function produce(): array
    {
        $context = [];
        $broker  = $this->getBroker();
        $config  = $this->getConfig();

        $requiredAck = $config->getRequiredAck();
        $timeout     = $config->getTimeout();
        $compression = $config->getCompression();

        // get send message
        // data struct
        //  topic:
        //  partId:
        //  key:
        //  value:
        $data = ($this->producer)();

        if (empty($data)) {
            return $context;
        }

        $sendData = $this->convertRecordSet($data);

        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect((string) $brokerId);

            if ($connect === null) {
                return $context;
            }

            $params = [
                'required_ack' => $requiredAck,
                'timeout'      => $timeout,
                'data'         => $topicList,
                'compression'  => $compression,
            ];

            $this->debug('Send message start, params:' . json_encode($params));
            $requestData = Protocol::encode(Protocol::PRODUCE_REQUEST, $params);

            if ($requiredAck === 0) { // If it is 0 the server will not send any response
                $this->state->succRun(State::REQUEST_PRODUCE);
            } else {
                $connect->write($requestData);
                $context[] = (int) $connect->getSocket();
            }
        }

        return $context;
    }

    /**
     * @param mixed[] $result
     */
    protected function succProduce(array $result, int $fd): void
    {
        $msg = sprintf('Send message sucess, result: %s', json_encode($result));
        $this->debug($msg);

        if ($this->success !== null) {
            ($this->success)($result);
        }

        $this->state->succRun(State::REQUEST_PRODUCE, $fd);
    }

    protected function stateConvert(int $errorCode): bool
    {
        $this->error(Protocol::getError($errorCode));

        if ($this->error !== null) {
            ($this->error)($errorCode);
        }

        $recoverCodes = [
            Protocol::UNKNOWN_TOPIC_OR_PARTITION,
            Protocol::INVALID_REQUIRED_ACKS,
            Protocol::RECORD_LIST_TOO_LARGE,
            Protocol::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            Protocol::NOT_ENOUGH_REPLICAS,
            Protocol::NOT_LEADER_FOR_PARTITION,
            Protocol::BROKER_NOT_AVAILABLE,
            Protocol::GROUP_LOAD_IN_PROGRESS,
            Protocol::GROUP_COORDINATOR_NOT_AVAILABLE,
            Protocol::NOT_COORDINATOR_FOR_GROUP,
            Protocol::INVALID_TOPIC,
            Protocol::INCONSISTENT_GROUP_PROTOCOL,
            Protocol::INVALID_GROUP_ID,
        ];

        if (in_array($errorCode, $recoverCodes, true)) {
            $this->state->recover();

            return false;
        }

        return true;
    }

    /**
     * @param mixed[] $recordSet
     *
     * @return mixed[]
     */
    protected function convertRecordSet(array $recordSet): array
    {
        $sendData = [];
        $broker   = $this->getBroker();
        $topics   = $broker->getTopics();

        foreach ($recordSet as $record) {
            $this->recordValidator->validate($record, $topics);

            $topicMeta = $topics[$record['topic']];
            $partId    = $broker->getPartitionId($record);

            $brokerId  = $topicMeta[$partId];
            $topicData = [];
            if (isset($sendData[$brokerId][$record['topic']])) {
                $topicData = $sendData[$brokerId][$record['topic']];
            }

            $partition = [];
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;
            if (trim($record['key'] ?? '') !== '') {
                $partition['messages'][] = ['value' => $record['value'], 'key' => $record['key']];
            } else {
                $partition['messages'][] = $record['value'];
            }

            $topicData['partitions'][$partId]      = $partition;
            $topicData['topic_name']               = $record['topic'];
            $sendData[$brokerId][$record['topic']] = $topicData;
        }

        return $sendData;
    }

    private function getConfig(): ProducerConfig
    {
        return ProducerConfig::getInstance();
    }

    private function getBroker(): Broker
    {
        return Broker::getInstance();
    }
}
