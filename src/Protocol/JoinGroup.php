<?php

namespace Kafka\Protocol;

class JoinGroup extends Protocol
{
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['group_id'])) {
            throw new \Kafka\Exception\Protocol('given join group data invalid. `group_id` is undefined.');
        }

        if (! isset($payloads['session_timeout'])) {
            throw new \Kafka\Exception\Protocol('given join group data invalid. `session_timeout` is undefined.');
        }

        if (! isset($payloads['member_id'])) {
            throw new \Kafka\Exception\Protocol('given join group data invalid. `member_id` is undefined.');
        }

        if (! isset($payloads['data'])) {
            throw new \Kafka\Exception\Protocol('given join group data invalid. `data` is undefined.');
        }

        if (! isset($payloads['protocol_type'])) {
            $payloads['protocol_type'] = 'consumer';
        }

        if (! isset($payloads['rebalance_timeout'])) {
            $payloads['rebalance_timeout'] = $payloads['session_timeout'];
        }

        $header = $this->requestHeader('kafka-php', self::JOIN_GROUP_REQUEST, self::JOIN_GROUP_REQUEST);
        $data   = self::encodeString($payloads['group_id'], self::PACK_INT16);
        $data  .= self::pack(self::BIT_B32, $payloads['session_timeout']);

        if ($this->getApiVersion(self::JOIN_GROUP_REQUEST) === self::API_VERSION1) {
            $data .= self::pack(self::BIT_B32, $payloads['rebalance_timeout']);
        }

        $data .= self::encodeString($payloads['member_id'], self::PACK_INT16);
        $data .= self::encodeString($payloads['protocol_type'], self::PACK_INT16);
        $data .= self::encodeArray($payloads['data'], [$this, 'encodeGroupProtocol']);
        $data  = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    public function decode(string $data): array
    {
        $offset        = 0;
        $errorCode     = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset       += 2;
        $generationId  = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset       += 4;
        $groupProtocol = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset       += $groupProtocol['length'];
        $leaderId      = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset       += $leaderId['length'];
        $memberId      = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset       += $memberId['length'];

        $members = $this->decodeArray(substr($data, $offset), [$this, 'joinGroupMember']);
        $offset += $memberId['length'];

        return [
            'errorCode'     => $errorCode,
            'generationId'  => $generationId,
            'groupProtocol' => $groupProtocol['data'],
            'leaderId'      => $leaderId['data'],
            'memberId'      => $memberId['data'],
            'members'       => $members['data'],
        ];
    }

    protected function encodeGroupProtocol(array $values): string
    {
        if (! isset($values['protocol_name'])) {
            throw new \Kafka\Exception\Protocol('given join group data invalid. `protocol_name` is undefined.');
        }

        $protocolName = self::encodeString($values['protocol_name'], self::PACK_INT16);

        if (! isset($values['version'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `version` is undefined.');
        }

        if (! isset($values['subscription']) || empty($values['subscription'])) {
            throw new \Kafka\Exception\Protocol('given data invalid. `subscription` is undefined.');
        }
        if (! isset($values['user_data'])) {
            $values['user_data'] = '';
        }

        $data  = self::pack(self::BIT_B16, '0');
        $data .= self::encodeArray($values['subscription'], [$this, 'encodeGroupProtocolMetaTopic']);
        $data .= self::encodeString($values['user_data'], self::PACK_INT32);

        return $protocolName . self::encodeString($data, self::PACK_INT32);
    }

    protected function encodeGroupProtocolMetaTopic(string $values): string
    {
        return self::encodeString($values, self::PACK_INT16);
    }

    protected function joinGroupMember(string $data): array
    {
        $offset     = 0;
        $memberId   = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset    += $memberId['length'];
        $memberMeta = $this->decodeString(substr($data, $offset), self::BIT_B32);
        $offset    += $memberMeta['length'];

        $metaData    = $memberMeta['data'];
        $metaOffset  = 0;
        $version     = self::unpack(self::BIT_B16, substr($metaData, $metaOffset, 2));
        $metaOffset += 2;
        $topics      = $this->decodeArray(substr($metaData, $metaOffset), [$this, 'decodeString'], self::BIT_B16);
        $metaOffset += $topics['length'];
        $userData    = $this->decodeString(substr($metaData, $metaOffset), self::BIT_B32);

        return [
            'length' => $offset,
            'data'   => [
                'memberId'   => $memberId['data'],
                'memberMeta' => [
                    'version'  => $version,
                    'topics'   => $topics['data'],
                    'userData' => $userData['data'],
                ],
            ],
        ];
    }
}
