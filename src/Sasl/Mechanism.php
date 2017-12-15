<?php
namespace Kafka\Sasl;

use Kafka\Contracts\SocketInterface;
use Kafka\Exception;
use Kafka\Protocol;
use Kafka\Protocol\Protocol as ProtocolTool;

abstract class Mechanism implements \Kafka\Contracts\SaslMechanism
{

    public function authenticate(SocketInterface $socket): void
    {
        $this->handShake($socket, $this->getName());
        $this->performAuthentication($socket);
    }

    /**
     *
     * sasl authenticate hand shake
     *
     * @access protected
     * @return void
     */
    protected function handShake(SocketInterface $socket, string $mechanism) : void
    {
        $requestData = Protocol::encode(\Kafka\Protocol::SASL_HAND_SHAKE_REQUEST, $mechanism);
        $socket->writeBlocking($requestData);
        $dataLen = ProtocolTool::unpack(\Kafka\Protocol\Protocol::BIT_B32, $socket->readBlocking(4));
        
        $data          = $socket->readBlocking($dataLen);
        $correlationId = ProtocolTool::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $result        = Protocol::decode(\Kafka\Protocol::SASL_HAND_SHAKE_REQUEST, substr($data, 4));

        if ($result['errorCode'] !== Protocol::NO_ERROR) {
            throw new Exception(Protocol::getError($result['errorCode']));
        }
    }

    abstract protected function performAuthentication(SocketInterface $socket): void;
    abstract public function getName(): string;
}
