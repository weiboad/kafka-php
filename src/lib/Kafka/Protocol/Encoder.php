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

class Encoder extends Protocol
{
    // {{{ functions
    // {{{ public static function encodeString()

    /**
     * encode pack string type 
     * 
     * @param string $string 
     * @param int $bytes   self::PACK_INT32: int32 big endian order. self::PACK_INT16: int16 big endian order.
     * @static
     * @access public
     * @return string
     */
    public static function encodeString($string, $bytes, $compression = self::COMPRESSION_NONE)
    {
        $packLen = ($bytes == self::PACK_INT32) ? 'N' : 'n';
        switch ($compression) {
            case self::COMPRESSION_NONE:
                break;
            case self::COMPRESSION_GZIP:
                $string = gzencode($string);    
                break;
            case self::COMPRESSION_SNAPPY:
                throw new \Kafka\Exception\NotSupported('SNAPPY compression not yet implemented');  
            default:
                throw new \Kafka\Exception\NotSupported('Unknown compression flag: ' . $compression);  
        }
        return pack($packLen, strlen($string)) . $string; 
    }

    // }}}
    // {{{ public static function encodeArray()

    /**
     * encode key array 
     * 
     * @param array $array 
     * @param Callable $func 
     * @static
     * @access public
     * @return string
     */
    public static function encodeArray(array $array, $func, $options = null)
    {
        if (!is_callable($func, false)) {
            throw new \Kafka\Exception('encode array failed, given function is not callable.');
        } 

        $arrayCount = count($array);

        $body = '';
        foreach ($array as $value) {
            if (!is_null($options)) {
                $body .= call_user_func($func, $value, $options); 
            } else {
                $body .= call_user_func($func, $value); 
            }
        }

        return pack('N', $arrayCount) . $body; 
    }
     
    // }}}
    // {{{ public static function encodeMessageSet()

    /**
     * encode message set 
     * N.B., MessageSets are not preceded by an int32 like other array elements
     * in the protocol.
     * 
     * @param array $messages 
     * @static
     * @access public
     * @return string
     */
    public static function encodeMessageSet($messages, $compression)
    {
        if (!is_array($messages)) {
            $messages = array($messages);    
        } 
    
        $data = '';
        foreach ($messages as $message) {
            $tmpMessage = self::_encodeMessage($message, $compression); 

            // int64 -- message offset     Message
            $data .= self::packInt64(0) . self::encodeString($tmpMessage, self::PACK_INT32);
        }
        return $data;
    }
     
    // }}}
    // {{{ public static function requestHeader()

    /**
     * get request header 
     * 
     * @param string $clientId 
     * @param integer $correlationId 
     * @param integer $apiKey 
     * @static
     * @access public
     * @return void
     */
    public static function requestHeader($clientId, $correlationId, $apiKey)
    {
        // int16 -- apiKey int16 -- apiVersion int32 correlationId
        $binData = pack('nnN', $apiKey, self::API_VERSION, $correlationId);
        
        // concat client id
        $binData .= self::encodeString($clientId, self::PACK_INT16);

        return $binData;
    }
     
    // }}}
    // {{{ public static function buildProduceRequest()

    /**
     * buildProduceRequest 
     * 
     * @param array $payloads 
     * @static
     * @access public
     * @return void
     */
    public static function buildProduceRequest($payloads, $compression = self::COMPRESSION_NONE)
    {
        if (!isset($payloads['data'])) {
            throw new \Kafka\Exception('given procude data invalid. `data` is undefined.');
        } 

        if (!isset($payloads['required_ack'])) { 
            // default server will not send any response 
            // (this is the only case where the server will not reply to a request)
            $payloads['required_ack'] = 0;
        }

        if (!isset($payloads['timeout'])) {
            $payloads['timeout'] = 100; // default timeout 100ms
        }
        
        $header = self::requestHeader('kafka-php', 0, self::PRODUCE_REQUEST);
        $data   = pack('nN', $payloads['required_ack'], $payloads['timeout']);
        $data  .= self::encodeArray($payloads['data'], array(self, '_encodeProcudeTopic'), $compression);
        $data   = self::encodeString($header . $data, self::PACK_INT32);
        return $data;
    }

    // }}}
    // {{{ private static function _encodeMessage()

    /**
     * encode signal message 
     * 
     * @param string $message 
     * @static
     * @access private
     * @return string
     */
    private static function _encodeMessage($message, $compression = self::COMPRESSION_NONE)
    {
        // int8 -- magic  int8 -- attribute
        $data = pack('CC', self::MESSAGE_MAGIC, $compression);

        // message key 
        $data .= self::encodeString('', self::PACK_INT32);

        // message value
        $data .= self::encodeString($message, self::PACK_INT32, $compression);

        $crc = crc32($data);

        // int32 -- crc code  string data
        $message = pack('N', $crc) . $data;

        return $message;
    }

    // }}}
    // {{{ private static function _encodeProcudePartion()

    /**
     * encode signal part 
     * 
     * @param partions 
     * @static
     * @access private
     * @return string
     */
    private static function _encodeProcudePartion($values, $compression)
    {
        if (!isset($values['partition_id'])) {
            throw new \Kafka\Exception('given procude data invalid. `partition_id` is undefined.');
        }

        if (!isset($values['messages']) || empty($values['messages'])) {
            throw new \Kafka\Exception('given procude data invalid. `messages` is undefined.');
        }

        $data = pack('N', $values['partition_id']);
        $data .= self::encodeString(self::encodeMessageSet($values['messages'], $compression), self::PACK_INT32);

        return $data;
    }

    // }}}
    // {{{ private static function _encodeProcudeTopic()

    /**
     * encode signal topic 
     * 
     * @param partions 
     * @static
     * @access private
     * @return string
     */
    private static function _encodeProcudeTopic($values, $compression)
    {
        if (!isset($values['topic_name'])) {
            throw new \Kafka\Exception('given procude data invalid. `topic_name` is undefined.');
        }

        if (!isset($values['partitions']) || empty($values['partitions'])) {
            throw new \Kafka\Exception('given procude data invalid. `partitions` is undefined.');
        }

        $topic = self::encodeString($values['topic_name'], self::PACK_INT16);
        $partitions = self::encodeArray($values['partitions'], array(self, '_encodeProcudePartion'), $compression);

        return $topic . $partitions;
    }

    // }}}
    // }}}
} 
