<?php
declare(strict_types=1);

namespace KafkaTest\Base\Producer;

use Kafka\Exception\InvalidRecordInSet;
use Kafka\Producer\RecordValidator;
use PHPUnit\Framework\TestCase;

final class RecordValidatorTest extends TestCase
{
    /** @var RecordValidator */
    private $recordValidator;

    public function setUp(): void
    {
        $this->recordValidator = new RecordValidator();

        parent::setUp();
    }

    /**
     * @doesNotPerformAssertions
     * @phpcsSuppress SlevomatCodingStandard.TypeHints.TypeHintDeclaration.UselessDocComment
     */
    public function testValidRecordDoesNotThrowException(): void
    {
        $this->recordValidator->validate(['topic' => 'test', 'value' => 'a value'], ['test' => []]);
    }

    /**
     * @param mixed[] $record
     * @dataProvider invalidRecordThrowsExceptionDataProvider
     */
    public function testInvalidRecordThrowsException(string $expectedExceptionMessage, array $record): void
    {
            $this->expectException(InvalidRecordInSet::class);
            $this->expectExceptionMessage($expectedExceptionMessage);

        $this->recordValidator->validate($record, ['test' => []]);
    }

    /**
     * @return mixed[][]
     */
    public function invalidRecordThrowsExceptionDataProvider(): array
    {
        return [
            'missing topic'                => ['You have to set "topic" to your message.', ['value' => 'a value']],
            'missing topic – empty string' => ['You have to set "topic" to your message.', ['topic' => '', 'value' => 'a value']],
            'invalid topic type'           => ['Topic must be string.', ['topic' => 1, 'value' => 'a value']],
            'missing value'                => ['You have to set "value" to your message.', ['topic' => 'test']],
            'invalid value type'           => ['Value must be string.', ['topic' => 'test', 'value' => 1]],
            'missing value – empty string' => ['You have to set "value" to your message.', ['topic' => 'test', 'value' => '']],
            'non - existing topic'         => [
                'Requested topic "non - existing topic" does not exist. Did you forget to create it?',
                [
                    'topic' => 'non - existing topic',
                    'value' => 'a value',
                ],
            ],
        ];
    }
}
