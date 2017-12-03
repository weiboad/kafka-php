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

namespace Kafka\Producer;

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

class SyncProcess
{
    use \Psr\Log\LoggerAwareTrait;
    use \Kafka\LoggerTrait;

    // {{{ consts
    // }}}
    // {{{ members
    // }}}
    // {{{ functions
    // {{{ public function __construct()

    public function __construct()
    {
        // init protocol
        $config = \Kafka\ProducerConfig::getInstance();
        \Kafka\Protocol::init($config->getBrokerVersion(), $this->logger);
        $this->syncMeta();
    }

    // }}}
    // {{{ public function send()

    public function send($data)
    {
        $broker = \Kafka\Broker::getInstance();
        $requiredAck = \Kafka\ProducerConfig::getInstance()->getRequiredAck();
        $timeout = \Kafka\ProducerConfig::getInstance()->getTimeout();

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
        $result = array();
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect($brokerId, true);
            if (!$connect) {
                return false;
            }

            $requiredAck = \Kafka\ProducerConfig::getInstance()->getRequiredAck();
            $params = array(
                'required_ack' => $requiredAck,
                'timeout' => \Kafka\ProducerConfig::getInstance()->getTimeout(),
                'data' => $topicList,
            );
            $this->debug("Send message start, params:" . json_encode($params));
            $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::PRODUCE_REQUEST, $params);
            $connect->write($requestData);
            if ($requiredAck != 0) { // If it is 0 the server will not send any response
                $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $connect->read(4));
                $data = $connect->read($dataLen);
                $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $ret = \Kafka\Protocol::decode(\Kafka\Protocol::PRODUCE_REQUEST, substr($data, 4));
                $result[] = $ret;
            }
        }
        return $result;
    }

    // }}}
    // {{{ public function syncMeta()

    public function syncMeta()
    {
        $this->debug('Start sync metadata request');
        $brokerList = explode(',', \Kafka\ProducerConfig::getInstance()->getMetadataBrokerList());
        $brokerHost = array();
        foreach ($brokerList as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }
        if (count($brokerHost) == 0) {
            throw new \Kafka\Exception('Not set config `metadataBrokerList`');
        }
        shuffle($brokerHost);
        $broker = \Kafka\Broker::getInstance();
        foreach ($brokerHost as $host) {
            $socket = $broker->getMetaConnect($host, true);
            if ($socket) {
                $params = array();
                $this->debug('Start sync metadata request params:' . json_encode($params));
                $requestData = \Kafka\Protocol::encode(\Kafka\Protocol::METADATA_REQUEST, $params);
                $socket->write($requestData);
                $dataLen = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, $socket->read(4));
                $data = $socket->read($dataLen);
                $correlationId = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $result = \Kafka\Protocol::decode(\Kafka\Protocol::METADATA_REQUEST, substr($data, 4));
                if (!isset($result['brokers']) || !isset($result['topics'])) {
                    throw new \Kafka\Exception('Get metadata is fail, brokers or topics is null.');
                } else {
                    $broker = \Kafka\Broker::getInstance();
                    $broker->setData($result['topics'], $result['brokers']);
                }
                return;
            }
        }
        throw new \Kafka\Exception('Not has broker can connection `metadataBrokerList`');
    }

    // }}}
    // {{{ protected function convertMessage()

    protected function convertMessage($data)
    {
        $sendData = array();
        $broker = \Kafka\Broker::getInstance();
        $topicInfos = $broker->getTopics();
        foreach ($data as $value) {
            if (!isset($value['topic']) || !trim($value['topic'])) {
                continue;
            }

            if (!isset($topicInfos[$value['topic']])) {
                continue;
            }

            if (!isset($value['value']) || !trim($value['value'])) {
                continue;
            }

            if (!isset($value['key'])) {
                $value['key'] = '';
            }

            $topicMeta = $topicInfos[$value['topic']];
            $partNums = array_keys($topicMeta);
            shuffle($partNums);
            $partId = 0;
            if (!isset($value['partId']) || !isset($topicMeta[$value['partId']])) {
                $partId = $partNums[0];
            } else {
                $partId = $value['partId'];
            }

            $brokerId = $topicMeta[$partId];
            $topicData = array();
            if (isset($sendData[$brokerId][$value['topic']])) {
                $topicData = $sendData[$brokerId][$value['topic']];
            }

            $partition = array();
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;
            if (trim($value['key']) != '') {
                $partition['messages'][] = array('value' => $value['value'], 'key' => $value['key']);
            } else {
                $partition['messages'][] = $value['value'];
            }
            
            $topicData['partitions'][$partId] = $partition;
            $topicData['topic_name'] = $value['topic'];
            $sendData[$brokerId][$value['topic']] = $topicData;
        }

        return $sendData;
    }

    // }}}
    // }}}
}
