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
    // {{{ public function decodeProduceResponse()

    /**
     * decode produce response 
     * 
     * @param string $data 
     * @access public
     * @return array
     */
    public function decodeProduceResponse($data)
    {
        //  data -> 0000000000000002000574657374360000000200000002000000000000000000140000000500000000000000000014000474657374000000010000000000000000000000000033
        $result = array();
        $offset = 4;
        $topicCount = unpack('N', substr($data, $offset, 4));
        $offset += 4;
        $topicCount = isset($topicCount[1]) ? $topicCount[1] : 0;
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = unpack('n', substr($data, $offset, 2)); // int16 topic name length	
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;	
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = unpack('N', substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = unpack('N', substr($data, $offset, 4));
                $offset += 4;
                $errCode     = unpack('n', substr($data, $offset, 2));
                $offset += 2;
                $partitionOffset = self::unpackInt64(substr($data, $offset, 8));
                $offset += 8;
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                    'offset'  => $partitionOffset
                );
            }
        }
        
        return $result; 
    }

    // }}}
    // {{{ public function decodeMetaDataResponse()

    /**
     * decode metadata response 
     * 
     * @param string $data 
     * @access public
     * @return array
     */
    public function decodeMetaDataResponse($data)
    {
        $result = array();
        $broker = array();
        $topic = array();
        $offset = 4;
        $brokerCount = unpack('N', substr($data, $offset, 4));
        $offset += 4;
        $brokerCount = isset($brokerCount[1]) ? $brokerCount[1] : 0;
        for ($i = 0; $i < $brokerCount; $i++) {
            $nodeId = unpack('N', substr($data, $offset, 4));
            $nodeId = $nodeId[1];
            $offset += 4;
            $hostNameLen = unpack('n', substr($data, $offset, 2)); // int16 host name length	
            $hostNameLen = isset($hostNameLen[1]) ? $hostNameLen[1] : 0;	
            $offset += 2;
            $hostName = substr($data, $offset, $hostNameLen);
            $offset += $hostNameLen;
            $port = unpack('N', substr($data, $offset, 4));
            $offset += 4;
            $broker[$nodeId] = array(
                'host' => $hostName,
                'port' => $port[1],
            );
        }
        
        $topicMetaCount = unpack('N', substr($data, $offset, 4));
        $offset += 4;
        $topicMetaCount = isset($topicMetaCount[1]) ? $topicMetaCount[1] : 0;
        for ($i = 0; $i < $topicMetaCount; $i++) {
            $topicErrCode = unpack('n', substr($data, $offset, 2));
            $offset += 2;
            $topicLen = unpack('n', substr($data, $offset, 2));
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen[1]);
            $offset += $topicLen[1];
            $partitionCount = unpack('N', substr($data, $offset, 4));
            $offset += 4;
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $topic[$topicName]['errCode'] = $topicErrCode[1];
            $partitions = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionErrCode = unpack('n', substr($data, $offset, 2));
                $offset += 2;
                $partitionId = unpack('N', substr($data, $offset, 4));
                $partitionId = isset($partitionId[1]) ? $partitionId[1] : 0;
                $offset += 4;
                $leaderId = unpack('N', substr($data, $offset, 4));
                $offset += 4;
                $repliasCount = unpack('N', substr($data, $offset, 4));
                $offset += 4;
                $repliasCount = isset($repliasCount[1]) ? $repliasCount[1] : 0; 
                $replias = array();
                for ($z = 0; $z < $repliasCount; $z++) {
                    $repliaId = unpack('N', substr($data, $offset, 4));
                    $offset += 4;    
                    $replias[] = $repliaId[1];
                }
                $isrCount = unpack('N', substr($data, $offset, 4));
                $offset += 4;
                $isrCount = isset($isrCount[1]) ? $isrCount[1] : 0; 
                $isrs = array();
                for ($z = 0; $z < $isrCount; $z++) {
                    $isrId = unpack('N', substr($data, $offset, 4));
                    $offset += 4;    
                    $isrs[] = $isrId[1];
                }

                $partitions[$partitionId] = array(
                    'errCode'  => $partitionErrCode[1],
                    'leader'   => $leaderId[1],
                    'replicas' => $replias,
                    'isr'      => $isrs,
                );
            }
            $topic[$topicName]['partitions'] = $partitions;
        }

        $result = array(
            'brokers' => $broker,
            'topics'  => $topic,
        );
        return $result; 
    }

    // }}}
    // {{{ public function decodeOffsetResponse()

    /**
     * decode offset response 
     * 
     * @param string $data 
     * @access public
     * @return array
     */
    public function decodeOffsetResponse($data)
    {
        $result = array();
        $offset = 4;
        $topicCount = unpack('N', substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = unpack('n', substr($data, $offset, 2)); // int16 topic name length	
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;	
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = unpack('N', substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = unpack('N', substr($data, $offset, 4));
                $offset += 4;
                $errCode     = unpack('n', substr($data, $offset, 2));
                $offset += 2;
                $offsetCount = unpack('N', substr($data, $offset, 4)); 
                $offset += 4;
                $offsetCount = array_shift($offsetCount);
                $offsetArr = array();
                for ($z = 0; $z < $offsetCount; $z++) {
                    $offsetArr[] = self::unpackInt64(substr($data, $offset, 8));
                    $offset += 8;
                }
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                    'offset'  => $offsetArr
                );
            }
        }
        return $result; 
    }

    // }}}
    // }}}
}
