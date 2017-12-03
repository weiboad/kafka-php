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

namespace KafkaTest;

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

class ProtocolTest extends \PHPUnit\Framework\TestCase
{
    // {{{ consts
    // }}}
    // {{{ members
    // }}}
    // {{{ functions
    // {{{ public function testEncode()

    /**
     * testEncode
     *
     * @access public
     * @return void
     */
    public function testEncode()
    {
        \Kafka\Protocol::init('0.9.0.1');
        $data = array(
            'group_id' => 'test',
            'member_id' => 'kafka-php-0e7cbd33-7950-40af-b691-eceaa665d297',
            'generation_id' => 2,
        );
        $test = \Kafka\Protocol::encode(\Kafka\Protocol::HEART_BEAT_REQUEST, $data);
        $this->assertEquals(\bin2hex($test), '0000004d000c00000000000c00096b61666b612d70687000047465737400000002002e6b61666b612d7068702d30653763626433332d373935302d343061662d623639312d656365616136363564323937');
    }

    // }}}
    // {{{ public function testEncodeNoKey()

    /**
     * testEncodeNoKey
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Not support api key, key:999
     * @access public
     * @return void
     */
    public function testEncodeNoKey()
    {
        \Kafka\Protocol::init('0.9.0.1');
        $data = array(
            'group_id' => 'test',
            'member_id' => 'kafka-php-0e7cbd33-7950-40af-b691-eceaa665d297',
            'generation_id' => 2,
        );
        $test = \Kafka\Protocol::encode(999, $data);
    }

    // }}}
    // {{{ public function testDecodeNoKey()

    /**
     * testDecodeNoKey
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Not support api key, key:999
     * @access public
     * @return void
     */
    public function testDecodeNoKey()
    {
        \Kafka\Protocol::init('0.9.0.1');
        $data = '';
        $test = \Kafka\Protocol::decode(999, $data);
    }

    // }}}
    // {{{ public function testDecode()

    /**
     * testDecode
     *
     * @access public
     * @return void
     */
    public function testDecode()
    {
        \Kafka\Protocol::init('0.9.0.1');
        $test = \Kafka\Protocol::decode(\Kafka\Protocol::HEART_BEAT_REQUEST, \hex2bin('0000'));
        $result = '{"errorCode":0}';
        $this->assertEquals(json_encode($test), $result);
    }

    // }}}
    // {{{ public function testError()

    /**
     * testError
     *
     * @access public
     * @return void
     */
    public function testError()
    {
        $errors = array(
            0 => 'No error--it worked!',
            -1 => 'An unexpected server error',
            1 => 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.',
            2 => 'This indicates that a message contents does not match its CRC',
            3 => 'This request is for a topic or partition that does not exist on this broker.',
            4 => 'The message has a negative size',
            5 => 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes',
            6 => 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.',
            7 => 'This error is thrown if the request exceeds the user-specified time limit in the request.',
            8 => 'This is not a client facing error and is used only internally by intra-cluster broker communication.',
            9 => 'The replica is not available for the requested topic-partition',
            10 => 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.',
            11 => 'Internal error code for broker-to-broker communication.',
            12 => 'If you specify a string larger than configured maximum for offset metadata',
            13 => 'The server disconnected before a response was received.',
            14 => 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).',
            15 => 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.',
            16 => 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.',
            17 => 'The request attempted to perform an operation on an invalid topic.',
            18 => 'The request included message batch larger than the configured segment size on the server.',
            19 => 'Messages are rejected since there are fewer in-sync replicas than required.',
            20 => 'Messages are written to the log, but to fewer in-sync replicas than required.',
            21 => 'Produce request specified an invalid value for required acks.',
            22 => 'Specified group generation id is not valid.',
            23 => 'The group member\'s supported protocols are incompatible with those of existing members.',
            24 => 'The configured groupId is invalid',
            25 => 'The coordinator is not aware of this member.',
            26 => 'The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).',
            27 => 'The group is rebalancing, so a rejoin is needed.',
            28 => 'The committing offset data size is not valid',
            29 => 'Topic authorization failed.',
            30 => 'Group authorization failed.',
            31 => 'Cluster authorization failed.',
            32 => 'The timestamp of the message is out of acceptable range.',
            33 => 'The broker does not support the requested SASL mechanism.',
            34 => 'Request is not valid given the current SASL state.',
            35 => 'The version of API is not supported.',
            999 => 'Unknown error'
        );

        foreach ($errors as $key => $value) {
            $result = \Kafka\Protocol::getError($key);
            $this->assertEquals($result, $value);
        }
    }

    // }}}
    // }}}
}
