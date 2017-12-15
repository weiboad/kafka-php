<?php
namespace Kafka\Producer;

use Psr\Log\LoggerInterface;
use Kafka\Contracts\Config\Broker as BrokerConfigInterface;
use Kafka\Contracts\Config\Producer as ProducerConfigInterface;
use Kafka\Contracts\BrokerInterface;
use Kafka\Contracts\Producer\SyncInterface;

class SyncProcess implements SyncInterface
{
    private $brokerConfig;

    private $producerConfig;

    private $broker;

    private $logger;

    public function __construct(
        BrokerInterface $broker,
        BrokerConfigInterface $brokerConfig,
        ProducerConfigInterface $producerConfig,
        LoggerInterface $logger
    ) {
        $this->logger         = $logger;
        $this->broker         = $broker;
        $this->brokerConfig   = $brokerConfig;
        $this->producerConfig = $producerConfig;

        // init protocol
        \Kafka\Protocol::init($this->brokerConfig->getVersion());
        // init broker
        $this->syncMeta();
    }

    public function send($data)
    {
        $requiredAck = $this->producerConfig->getRequiredAck();
        $timeout     = $this->producerConfig->getTimeout();

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
            $connect = $this->broker->getDataConnect($brokerId, true);
            if (! $connect) {
                return false;
            }

            $params = [
                'required_ack' => $requiredAck,
                'timeout' => $timeout,
                'data' => $topicList,
            ];
            $this->logger->debug("Send message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);
            $connect->write($requestData);
            if ($requiredAck != 0) { // If it is 0 the server will not send any response
                $dataLen       = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $connect->read(4));
                $data          = $connect->read($dataLen);
                $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $ret           = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
                $result[]      = $ret;
            }
        }
        return $result;
    }

    public function syncMeta()
    {
        $this->logger->debug('Start sync metadata request');
        $brokerList = $this->brokerConfig->getMetadataBrokerList();
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
        foreach ($brokerHost as $host) {
            $socket = $this->broker->getMetaConnect($host);
            if ($socket) {
                $params = [];
                $this->logger->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
                $socket->write($requestData);
                $dataLen       = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $socket->read(4));
                $data          = $socket->read($dataLen);
                $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $result        = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));
                if (! isset($result['brokers']) || ! isset($result['topics'])) {
                    throw new \Kafka\Exception('Get metadata is fail, brokers or topics is null.');
                } else {
                    $this->broker->setData($result['topics'], $result['brokers']);
                }
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

    protected function convertMessage($data)
    {
        $sendData   = [];
        $topicInfos = $this->broker->getTopics();
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

            if (! isset($value['key'])) {
                $value['key'] = '';
            }

            $topicMeta = $topicInfos[$value['topic']];
            $partNums  = array_keys($topicMeta);
            shuffle($partNums);
            $partId = 0;
            if (! isset($value['partId']) || ! isset($topicMeta[$value['partId']])) {
                $partId = $partNums[0];
            } else {
                $partId = $value['partId'];
            }

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
            if (trim($value['key']) != '') {
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
