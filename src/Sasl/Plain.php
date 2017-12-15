<?php

namespace Kafka\Sasl;

use Kafka\Contracts\Config\Sasl;
use Kafka\Contracts\SocketInterface;
use Kafka\SaslMechanism;
use Kafka\Exception;

class Plain extends Mechanism
{
    private const MECHANISM_NAME = "PLAIN";

    private $config;

    
    /**
     *
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct(Sasl $config)
    {
        $this->config = $config;
    }

    /**
     *
     * sasl authenticate
     *
     * @access protected
     * @return void
     */
    protected function performAuthentication(SocketInterface $socket) : void
    {
        $split = \Kafka\Protocol\Protocol::pack(\Kafka\Protocol\Protocol::BIT_B8, 0);
        $data  = $split . $this->config->getUsername() . $split . $this->config->getPassword();
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
