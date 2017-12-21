<?php
namespace Kafka\Contracts;

interface SaslMechanism
{
    /**
     *
     * sasl authenticate
     *
     * @access public
     * @return void
     */
    public function authenticate(SocketInterface $socket) : void;
}
