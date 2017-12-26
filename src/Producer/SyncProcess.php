<?php
namespace Kafka\Producer;

use Kafka\Broker;
use Kafka\ProducerConfig;
use Kafka\Protocol\Protocol;
use Psr\Log\LoggerAwareTrait;
use Kafka\LoggerTrait;
use Kafka\Exception;

class SyncProcess
{
    use LoggerAwareTrait;
    use LoggerTrait;

    public function __construct()
    {
        // init protocol
        $config = ProducerConfig::getInstance();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);
        // init broker
        $broker = Broker::getInstance();
        $broker->setConfig($config);

        $this->syncMeta();
    }

    public function send($data)
    {
        $broker      = Broker::getInstance();
        $requiredAck = ProducerConfig::getInstance()->getRequiredAck();
        $timeout     = ProducerConfig::getInstance()->getTimeout();
        $compression = ProducerConfig::getInstance()->getCompression();

        // get send message
        // data struct
        //  topic:
        //  partId:
        //  key:
        //  value:
        if (empty($data)) {
            return false;
        }

        $sendData = $this->convertMessage($data);
        $result   = [];
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect($brokerId, true);

            if (! $connect) {
                return false;
            }

            $params = [
                'required_ack' => $requiredAck,
                'timeout'      => $timeout,
                'data'         => $topicList,
                'compression'  => $compression,
            ];

            $this->debug("Send message start, params:" . json_encode($params));
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

    public function syncMeta()
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
        $broker = Broker::getInstance();

        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host, true);

            if (! $socket) {
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
                throw new \Kafka\Exception('Get metadata is fail, brokers or topics is null.');
            }

            $broker = Broker::getInstance();
            $broker->setData($result['topics'], $result['brokers']);

            return;
        }

        throw new \Kafka\Exception(
            sprintf(
                'It was not possible to establish a connection for metadata with the brokers "%s"',
                $brokerList
            )
        );
    }

    protected function convertMessage(array $data): array
    {
        $sendData   = [];
        $broker     = Broker::getInstance();
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
}
