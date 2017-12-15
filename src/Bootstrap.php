<?php
namespace Kafka;

use Kafka\Exception;
use DI\FactoryInterface;
use DI\ContainerBuilder;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

class Bootstrap
{

    public static function getContainer(array $definition = []) : FactoryInterface
    {
        $builder = new \DI\ContainerBuilder();
        $builder->useAnnotations(false);

        $definition = array_merge(self::containerDefinition(), $definition);
        $builder->addDefinitions($definition);
        return $builder->build();
    }

    protected static function containerDefinition() : array
    {
        return [
            LoggerInterface::class => \DI\object(NullLogger::class),
            \Kafka\Contracts\Producer\SyncInterface::class => \DI\object(\Kafka\Producer\SyncProcess::class),
            \Kafka\Contracts\Consumer\Process::class => \DI\object(\Kafka\Consumer\Process::class),
            \Kafka\Contracts\Consumer\State::class => \DI\object(\Kafka\Consumer\State::class),
            \Kafka\Contracts\Consumer\Assignment::class => \DI\object(\Kafka\Consumer\Assignment::class),
            \Kafka\Contracts\BrokerInterface::class => \DI\object(\Kafka\Broker::class),
            \Kafka\Contracts\SocketInterface::class => \DI\object(\Kafka\Socket\SocketBlocking::class),
            \Kafka\Contracts\SaslMechanism::class => \DI\object(\Kafka\Sasl\Plain::class),
            \Kafka\Contracts\Config\Broker::class => \DI\object(\Kafka\Config\Broker::class),
            \Kafka\Contracts\Config\Socket::class => \DI\object(\Kafka\Config\Socket::class),
            \Kafka\Contracts\Config\Ssl::class => \DI\object(\Kafka\Config\Ssl::class),
            \Kafka\Contracts\Config\Sasl::class => \DI\object(\Kafka\Config\Sasl::class),
            \Kafka\Contracts\Config\Producer::class => \DI\object(\Kafka\Config\Producer::class),
            \Kafka\Contracts\Config\Consumer::class => \DI\object(\Kafka\Config\Consumer::class),
            \Kafka\Contracts\Consumer\StopStrategy::class => function () {
                return null;
            },
        ];
    }
}
