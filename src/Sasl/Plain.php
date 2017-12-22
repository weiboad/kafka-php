<?php

namespace Kafka\Sasl;

use Kafka\CommonSocket;
use Kafka\SaslMechanism;
use Kafka\Exception;

class Plain extends Mechanism
{
    private const MECHANISM_NAME = "PLAIN";

    private $username;

    private $password;

    
    /**
     *
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct(string $username, string $password)
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
    protected function performAuthentication(CommonSocket $socket) : void
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
    public function getName() : string
    {
        return self::MECHANISM_NAME;
    }
}
