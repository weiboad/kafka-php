<?php

namespace Kafka\Protocol;

class Metadata extends Protocol
{
    public function encode(array $payloads = []): string
    {
        foreach ($payloads as $topic) {
            if (! \is_string($topic)) {
                throw new \Kafka\Exception\Protocol('request metadata topic array have invalid value. ');
            }
        }

        $header = $this->requestHeader('kafka-php', self::METADATA_REQUEST, self::METADATA_REQUEST);
        $data   = self::encodeArray($payloads, [$this, 'encodeString'], self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    public function decode(string $data): array
    {
        $offset       = 0;
        $version      = $this->getApiVersion(self::METADATA_REQUEST);
        $brokerRet    = $this->decodeArray(substr($data, $offset), [$this, 'metaBroker'], $version);
        $offset      += $brokerRet['length'];
        $topicMetaRet = $this->decodeArray(substr($data, $offset), [$this, 'metaTopicMetaData'], $version);
        $offset      += $topicMetaRet['length'];

        $result = [
            'brokers' => $brokerRet['data'],
            'topics'  => $topicMetaRet['data'],
        ];

        return $result;
    }

    protected function metaBroker(string $data, string $version): array
    {
        $offset       = 0;
        $nodeId       = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset      += 4;
        $hostNameInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset      += $hostNameInfo['length'];
        $port         = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset      += 4;

        return [
            'length' => $offset,
            'data'   => [
                'host'   => $hostNameInfo['data'],
                'port'   => $port,
                'nodeId' => $nodeId,
            ],
        ];
    }

    protected function metaTopicMetaData(string $data, string $version): array
    {
        $offset         = 0;
        $topicErrCode   = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset        += 2;
        $topicInfo      = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset        += $topicInfo['length'];
        $partitionsMeta = $this->decodeArray(substr($data, $offset), [$this, 'metaPartitionMetaData'], $version);
        $offset        += $partitionsMeta['length'];

        return [
            'length' => $offset,
            'data'   => [
                'topicName'  => $topicInfo['data'],
                'errorCode'  => $topicErrCode,
                'partitions' => $partitionsMeta['data'],
            ],
        ];
    }

    protected function metaPartitionMetaData(string $data, string $version): array
    {
        $offset   = 0;
        $errcode  = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset  += 2;
        $partId   = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset  += 4;
        $leader   = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset  += 4;
        $replicas = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset  += $replicas['length'];
        $isr      = $this->decodePrimitiveArray(substr($data, $offset), self::BIT_B32);
        $offset  += $isr['length'];

        return [
            'length' => $offset,
            'data'   => [
                'partitionId' => $partId,
                'errorCode'   => $errcode,
                'replicas'    => $replicas['data'],
                'leader'      => $leader,
                'isr'         => $isr['data'],
            ],
        ];
    }
}
