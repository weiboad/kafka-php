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
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Decoder extends Protocol
{
    // {{{ functions
    // {{{ public function produceResponse()

    /**
     * decode produce response
     *
     * @access public
     * @return array
     */
    public function produceResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('produce response invalid.');
        }
        $data = $this->stream->read($dataLen, true);

        // parse data struct
        $offset = 4;
        $version = $this->getApiVersion(self::PRODUCE_REQUEST);
        $ret = $this->decodeArray(substr($data, $offset), array($this, 'produceTopicPair'), $version);
        $offset += $ret['length'];
        $throttleTime = 0;
        if ($version == self::API_VERSION2) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        }
        return array('throttleTime' => $throttleTime, 'data' => $ret['data']);
    }

    // }}}
    // {{{ public function fetchResponse()

    /**
     * decode fetch response
     *
     * @access public
     * @return \Iterator
     */
    public function fetchResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('produce response invalid.');
        }
        $data = $this->stream->read($dataLen, true);

        // parse data struct
        $offset = 4;
    }

    // }}}
    
    // {{{ public function commitOffsetResponse()

    /**
     * decode commit offset response
     *
     * @access public
     * @return array
     */
    public function commitOffsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('commit offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $errCode     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                );
            }
        }
        return $result;
    }

    // }}}
    // {{{ public function fetchOffsetResponse()

    /**
     * decode fetch offset response
     *
     * @access public
     * @return array
     */
    public function fetchOffsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('fetch offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset += 8;
                $metaLen = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $metaLen = array_shift($metaLen);
                $offset += 2;
                $metaData = '';
                if ($metaLen) {
                    $metaData = substr($data, $offset, $metaLen);
                    $offset += $metaLen;
                }
                $errCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
                $offset += 2;
                $result[$topicName][$partitionId[1]] = array(
                    'offset'   => $partitionOffset,
                    'metadata' => $metaData,
                    'errCode'  => $errCode[1],
                );
            }
        }
        return $result;
    }

    // }}}
    
    // {{{ protected function produceTopicPair()

    /**
     * decode produce topic pair response
     *
     * @access protected
     * @return array
     */
    protected function produceTopicPair($data, $version)
    {
        $offset = 0;
        $topicInfo = $this->decodeString($data, self::BIT_B16);
        $offset += $topicInfo['length'];
        $ret = $this->decodeArray(substr($data, $offset), array($this, 'producePartitionPair'), $version);
        $offset += $ret['length'];

        return array('length' => $offset, 'data' => array(
            'topicName' => $topicInfo['data'],
            'partitions'=> $ret['data'],
        ));
    }

    // }}}
    // {{{ protected function producePartitionPair()

    /**
     * decode produce partition pair response
     *
     * @access protected
     * @return array
     */
    protected function producePartitionPair($data, $version)
    {
        $offset = 0;
        $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset += 2;
        $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
        $offset += 8;
        $timestamp = 0;
        if ($version == self::API_VERSION2) {
            $timestamp = self::unpack(self::BIT_B64, substr($data, $offset, 8));
            $offset += 8;
        }

        return array(
            'length' => $offset,
            'data'   => array(
                'partition' => $partitionId,
                'errorCode' => $errorCode,
                'offset' => $offset,
                'timestamp' => $timestamp,
            )
        );
    }

    // }}}

    
    
    // }}}
}
