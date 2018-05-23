<?php
declare(strict_types=1);

namespace Kafka;

class SocketFactory
{
    public function createSocket(
        string $host,
        int $port,
        ?Config $config = null,
        ?SaslMechanism $saslProvider = null
    ): Socket {
        return new Socket($host, $port, $config, $saslProvider);
    }

    public function createSocketSync(
        string $host,
        int $port,
        ?Config $config = null,
        ?SaslMechanism $saslProvider = null
    ): SocketSync {
        return new SocketSync($host, $port, $config, $saslProvider);
    }
}
