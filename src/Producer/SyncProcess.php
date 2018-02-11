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
use function sprintf;
use function substr;
use function trim;

class SyncProcess
{
    use LoggerAwareTrait;
    use LoggerTrait;

    public function __construct()
    {
        $config = $this->getConfig();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);

        $broker = $this->getBroker();
        $broker->setConfig($config);

        $this->syncMeta();
    }

    /**
     * @param string[][] $data
     *
     * @return mixed[]
     *
     * @throws \Kafka\Exception
     */
    public function send(array $data): array
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
        if (empty($data)) {
            return [];
        }

        $sendData = $this->convertMessage($data);
        $result   = [];
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect((string) $brokerId, true);

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
                $data          = $connect->read($dataLen);
                $correlationId = Protocol::unpack(Protocol::BIT_B32, substr($data, 0, 4));
                $ret           = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));

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
            $socket = $broker->getMetaConnect($host, true);

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

        throw new Exception(
            sprintf(
                'It was not possible to establish a connection for metadata with the brokers "%s"',
                $brokerList
            )
        );
    }

    /**
     * @param string[][] $data
     *
     * @return mixed[]
     */
    protected function convertMessage(array $data): array
    {
        $sendData   = [];
        $broker     = $this->getBroker();
        $topicInfos = $broker->getTopics();

        foreach ($data as $value) {
            if (! isset($value['topic']) || ! trim($value['topic'])) {
                continue;
            }

            if (! isset($topicInfos[$value['topic']])) {
                continue;
            }

            if (! isset($value['value']) || ! trim($value['value'])) {
                continue;
            }

            $topicMeta = $topicInfos[$value['topic']];
            $partNums  = array_keys($topicMeta);
            shuffle($partNums);

            $partId = isset($value['partId'], $topicMeta[$value['partId']]) ? $value['partId'] : $partNums[0];

            $brokerId  = $topicMeta[$partId];
            $topicData = [];
            if (isset($sendData[$brokerId][$value['topic']])) {
                $topicData = $sendData[$brokerId][$value['topic']];
            }

            $partition = [];
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;

            if (trim($value['key'] ?? '') !== '') {
                $partition['messages'][] = ['value' => $value['value'], 'key' => $value['key']];
            } else {
                $partition['messages'][] = $value['value'];
            }

            $topicData['partitions'][$partId]     = $partition;
            $topicData['topic_name']              = $value['topic'];
            $sendData[$brokerId][$value['topic']] = $topicData;
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
