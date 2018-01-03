<?php
declare(strict_types=1);

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;

class LeaveGroup extends Protocol
{
    /**
     * @param mixed[] $payloads
     *
     * @throws NotSupported
     * @throws ProtocolException
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['group_id'])) {
            throw new ProtocolException('given leave group data invalid. `group_id` is undefined.');
        }

        if (! isset($payloads['member_id'])) {
            throw new ProtocolException('given leave group data invalid. `member_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::LEAVE_GROUP_REQUEST, self::LEAVE_GROUP_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::encodeString($payloads['member_id'], self::PACK_INT16);

        return self::encodeString($header . $data, self::PACK_INT32);
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, 0, 2));

        return ['errorCode' => $errorCode];
    }
}
