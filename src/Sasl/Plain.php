<?php
declare(strict_types=1);

namespace Kafka\Sasl;

use Kafka\CommonSocket;
use Kafka\Protocol\Protocol;

class Plain extends Mechanism
{
    private const MECHANISM_NAME = 'PLAIN';

    /**
     * @var string
     */
    private $username;

    /**
     * @var string
     */
    private $password;


    public function __construct(string $username, string $password)
    {
        $this->username = \trim($username);
        $this->password = \trim($password);
    }

    protected function performAuthentication(CommonSocket $socket): void
    {
        $split = Protocol::pack(Protocol::BIT_B8, '0');

        $data = Protocol::encodeString(
            $split . $this->username . $split . $this->password,
            Protocol::PACK_INT32
        );

        $socket->writeBlocking($data);
        $socket->readBlocking(4);
    }

    public function getName(): string
    {
        return self::MECHANISM_NAME;
    }
}
