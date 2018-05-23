<?php
declare(strict_types=1);

namespace Kafka\Producer;

use Kafka\Broker;
use Kafka\Exception;
use Kafka\LoggerTrait;
use Kafka\ProducerConfig;
use Kafka\Protocol\Protocol;
use Psr\Log\LoggerAwareTrait;
use function array_keys;
use function count;
use function explode;
use function json_encode;
use function shuffle;
use function substr;
use function trim;

class SyncProcess
{
    use LoggerAwareTrait;
    use LoggerTrait;

    /** @var RecordValidator */
    private $recordValidator;

    public function __construct(?RecordValidator $recordValidator = null)
    {
        $this->recordValidator = $recordValidator ?? new RecordValidator();

        $config = $this->getConfig();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);

        $broker = $this->getBroker();
        $broker->setConfig($config);

        $this->syncMeta();
    }

    /**
     * @param mixed[][] $recordSet
     *
     * @return mixed[]
     *
     * @throws \Kafka\Exception
     */
    public function send(array $recordSet): array
    {
        $broker = $this->getBroker();
        $config = $this->getConfig();

        $requiredAck = $config->getRequiredAck();
        $timeout     = $config->getTimeout();
        $compression = $config->getCompression();

        // get send message
        // data struct
        //  topic:
        //  partId:
        //  key:
        //  value:
        if (empty($recordSet)) {
            return [];
        }

        $sendData = $this->convertRecordSet($recordSet);
        $result   = [];
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect((string) $brokerId, Broker::SOCKET_MODE_SYNC);

            if ($connect === null) {
                return [];
            }

            $params = [
                'required_ack' => $requiredAck,
                'timeout'      => $timeout,
                'data'         => $topicList,
                'compression'  => $compression,
            ];

            $this->debug('Send message start, params:' . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);
            $connect->write($requestData);

            if ($requiredAck !== 0) { // If it is 0 the server will not send any response
                $dataLen       = Protocol::unpack(Protocol::BIT_B32, $connect->read(4));
                $recordSet     = $connect->read($dataLen);
                $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($recordSet, 0, 4));
                $ret           = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($recordSet, 4));

                $result[] = $ret;
            }
        }

        return $result;
    }

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
            $socket = $broker->getMetaConnect($host, Broker::SOCKET_MODE_SYNC);

            if ($socket === null) {
                continue;
            }

            $params = [];
            $this->debug('Start sync metadata request params:' . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
            $socket->write($requestData);
            $dataLen       = Protocol::unpack(Protocol::BIT_B32, $socket->read(4));
            $data          = $socket->read($dataLen);
            $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($data, 0, 4));
            $result        = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));

            if (! isset($result['brokers'], $result['topics'])) {
                throw new Exception('Get metadata is fail, brokers or topics is null.');
            }

            $broker = $this->getBroker();
            $broker->setData($result['topics'], $result['brokers']);

            return;
        }

        throw Exception\ConnectionException::fromBrokerList($brokerList);
    }

    /**
     * @param string[][] $recordSet
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
            $partNums  = array_keys($topicMeta);
            shuffle($partNums);

            $partId = isset($record['partId'], $topicMeta[$record['partId']]) ? $record['partId'] : $partNums[0];

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

    private function getBroker(): Broker
    {
        return Broker::getInstance();
    }

    private function getConfig(): ProducerConfig
    {
        return ProducerConfig::getInstance();
    }
}
