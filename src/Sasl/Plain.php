<?php

namespace Kafka\Sasl;

use Kafka\connections\CommonSocket;
use Kafka\SaslMechanism;
use Kafka\Exception;

class Plain extends Mechanism
{
    const MECHANISM_NAME = "PLAIN";

    private $username;

    private $password;

    
    /**
     *
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct($username, $password)
    {
        $this->username = trim($username);
        $this->password = trim($password);
    }

    /**
     *
     * sasl authenticate
     *
     * @access protected
     * @return void
     */
    protected function performAuthentication(CommonSocket $socket)
    {
        $split = \Kafka\Protocol\Protocol::pack(\Kafka\Protocol\Protocol::BIT_B8, 0);
        $data  = $split . $this->username . $split . $this->password;
        $data  = \Kafka\Protocol\Protocol::encodeString($data, \Kafka\Protocol\Protocol::PACK_INT32);
        $socket->writeBlocking($data);
        $socket->readBlocking(4, true);
    }

    /**
     *
     * get sasl authenticate mechanism name
     *
     * @access public
     * @return string
     */
    public function getName()
    {
        return self::MECHANISM_NAME;
    }
}
