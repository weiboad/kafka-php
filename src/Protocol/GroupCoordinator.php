<?php
namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;

class GroupCoordinator extends Protocol
{
    /**
     * @param mixed[] $payloads
     *
     * @throws ProtocolException
     * @throws NotSupported
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['group_id'])) {
            throw new ProtocolException('given group coordinator invalid. `group_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::GROUP_COORDINATOR_REQUEST, self::GROUP_COORDINATOR_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset          = 0;
        $errorCode       = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset         += 2;
        $coordinatorId   = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset         += 4;
        $hosts           = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset         += $hosts['length'];
        $coordinatorPort = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset         += 4;

        return [
            'errorCode'       => $errorCode,
            'coordinatorId'   => $coordinatorId,
            'coordinatorHost' => $hosts['data'],
            'coordinatorPort' => $coordinatorPort,
        ];
    }
}
