<?php
declare(strict_types=1);

namespace Kafka\Protocol;

use Kafka\Exception\NotSupported;
use Kafka\Exception\Protocol as ProtocolException;

class Heartbeat extends Protocol
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
            throw new ProtocolException('given heartbeat data invalid. `group_id` is undefined.');
        }

        if (! isset($payloads['generation_id'])) {
            throw new ProtocolException('given heartbeat data invalid. `generation_id` is undefined.');
        }

        if (! isset($payloads['member_id'])) {
            throw new ProtocolException('given heartbeat data invalid. `member_id` is undefined.');
        }

        $header = $this->requestHeader('kafka-php', self::HEART_BEAT_REQUEST, self::HEART_BEAT_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::pack(self::BIT_B32, (string) $payloads['generation_id']);
        $data  .= self::encodeString($payloads['member_id'], self::PACK_INT16);

        $data = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * @return mixed[]
     */
    public function decode(string $data): array
    {
        $offset    = 0;
        $errorCode = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset   += 2;

        return ['errorCode' => $errorCode];
    }
}
