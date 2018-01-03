<?php
declare(strict_types=1);

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;

class ListGroup extends Protocol
{
    /**
     * @param mixed[] $payloads
     *
     * @throws NotSupported
     */
    public function encode(array $payloads = []): string
    {
        $header = $this->requestHeader('kafka-php', self::LIST_GROUPS_REQUEST, self::LIST_GROUPS_REQUEST);

        return self::encodeString($header, self::PACK_INT32);
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset    = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, \substr($data, $offset, 2));
        $offset   += 2;
        $groups    = $this->decodeArray(\substr($data, $offset), [$this, 'listGroup']);

        return [
            'errorCode' => $errorCode,
            'groups'    => $groups['data'],
        ];
    }

    /**
     * @return mixed[]
     */
    protected function listGroup(string $data): array
    {
        $offset       = 0;
        $groupId      = $this->decodeString(\substr($data, $offset), self::BIT_B16);
        $offset      += $groupId['length'];
        $protocolType = $this->decodeString(\substr($data, $offset), self::BIT_B16);
        $offset      += $protocolType['length'];

        return [
            'length' => $offset,
            'data'   => [
                'groupId'      => $groupId['data'],
                'protocolType' => $protocolType['data'],
            ],
        ];
    }
}
