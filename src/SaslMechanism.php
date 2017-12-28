<?php
namespace Kafka;

interface SaslMechanism
{
    /**
     *
     * sasl authenticate
     *
     * @access public
     */
    public function authenticate(CommonSocket $socket): void;
}
