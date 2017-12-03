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

namespace Kafka\Protocol;

/**
+------------------------------------------------------------------------------
* Kafka protocol for meta data api
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Metadata extends Protocol
{
    // {{{ functions
    // {{{ public function encode()

    /**
     * meta data request encode
     *
     * @param array $payloads
     * @access public
     * @return string
     */
    public function encode($topics)
    {
        if (!is_array($topics)) {
            $topics = array($topics);
        }

        foreach ($topics as $topic) {
            if (!is_string($topic)) {
                throw new \Kafka\Exception\Protocol('request metadata topic array have invalid value. ');
            }
        }

        $header = $this->requestHeader('kafka-php', self::METADATA_REQUEST, self::METADATA_REQUEST);
        $data   = self::encodeArray($topics, array($this, 'encodeString'), self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    // }}}
    // {{{ public function decode()

    /**
     * decode metadata response
     *
     * @access public
     * @return array
     */
    public function decode($data)
    {
        $offset = 0;
        $version = $this->getApiVersion(self::METADATA_REQUEST);
        $brokerRet = $this->decodeArray(substr($data, $offset), array($this, 'metaBroker'), $version);
        $offset += $brokerRet['length'];
        $topicMetaRet = $this->decodeArray(substr($data, $offset), array($this, 'metaTopicMetaData'), $version);
        $offset += $topicMetaRet['length'];

        $result = array(
            'brokers' => $brokerRet['data'],
            'topics'  => $topicMetaRet['data'],
        );
        return $result;
    }

    // }}}
    // {{{ protected function metaBroker()

    /**
     * decode meta broker response
     *
     * @access protected
     * @return array
     */
    protected function metaBroker($data, $version)
    {
        $offset = 0;
        $nodeId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $hostNameInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $hostNameInfo['length'];
        $port = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        return array(
            'length' => $offset,
            'data' => array(
                'host' => $hostNameInfo['data'],
                'port' => $port,
                'nodeId' => $nodeId
            )
        );
    }

    // }}}
    // {{{ protected function metaTopicMetaData()

    /**
     * decode meta topic meta data response
     *
     * @access protected
     * @return array
     */
    protected function metaTopicMetaData($data, $version)
    {
        $offset = 0;
        $topicErrCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $topicInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset += $topicInfo['length'];
        $partionsMetaRet = $this->decodeArray(substr($data, $offset), array($this, 'metaPartitionMetaData'), $version);
        $offset += $partionsMetaRet['length'];

        return array(
            'length' => $offset,
            'data' => array(
                'topicName' => $topicInfo['data'],
                'errorCode' => $topicErrCode,
                'partitions' => $partionsMetaRet['data'],
            )
        );
    }

    // }}}
    // {{{ protected function metaPartitionMetaData()

    /**
     * decode meta partition meta data response
     *
     * @access protected
     * @return array
     */
    protected function metaPartitionMetaData($data, $version)
    {
        $offset = 0;
        $errcode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $partId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $leader = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $replicas = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset += $replicas['length'];
        $isr = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset += $isr['length'];

        return array(
            'length' => $offset,
            'data' => array(
                'partitionId' => $partId,
                'errorCode' => $errcode,
                'replicas' => $replicas['data'],
                'leader' => $leader,
                'isr' => $isr['data'],
            )
        );
    }

    // }}}
    // }}}
}
