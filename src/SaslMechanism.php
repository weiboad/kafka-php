<?php
declare(strict_types=1);

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
