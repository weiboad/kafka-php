<?php
namespace Kafka;

use Kafka\connections\CommonSocket;

interface SaslMechanism
{
    /**
     *
     * sasl authenticate
     *
     * @access public
     * @return void
     */
    public function authenticate(CommonSocket $socket);
}
