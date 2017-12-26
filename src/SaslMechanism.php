<?php
namespace Kafka;

interface SaslMechanism
{
    /**
     *
     * sasl authenticate
     *
     * @access public
     * @return void
     */
    public function authenticate(CommonSocket $socket): void;
}
