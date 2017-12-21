<?php

namespace Kafka\Sasl;

use Kafka\Contracts\SocketInterface;
use Kafka\SaslMechanism;
use Kafka\Exception;

class NullSasl extends Mechanism
{
    private const MECHANISM_NAME = "NULL-SASL";

    /**
     *
     * sasl authenticate
     *
     * @access protected
     * @return void
     */
    protected function performAuthentication(SocketInterface $socket) : void
    {
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

    // @overwrite
    public function authenticate(SocketInterface $socket): void
    {
    }
}
