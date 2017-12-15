<?php
namespace Kafka\Consumer;

use DI\FactoryInterface;
use Kafka\Contracts\Consumer\Assignment as AssignmentInterface;

class Assignment implements AssignmentInterface
{
	private $assignments = [];

    public function assign(FactoryInterface $container, array $memberInfos) : void
    {
		$broker = $container->get(\Kafka\Contracts\BrokerInterface::class);
        $topics = $broker->getTopics();

        $memberCount = count($memberInfos);

        $count   = 0;
        $members = [];
        foreach ($topics as $topicName => $partition) {
            foreach ($partition as $partId => $leaderId) {
                $memberNum = $count % $memberCount;
                if (! isset($members[$memberNum])) {
                    $members[$memberNum] = [];
                }
                if (! isset($members[$memberNum][$topicName])) {
                    $members[$memberNum][$topicName] = [];
                }
                $members[$memberNum][$topicName]['topic_name'] = $topicName;
                if (! isset($members[$memberNum][$topicName]['partitions'])) {
                    $members[$memberNum][$topicName]['partitions'] = [];
                }
                $members[$memberNum][$topicName]['partitions'][] = $partId;
                $count++;
            }
        }

        $data = [];
        foreach ($memberInfos as $key => $member) {
            $item   = [
                'version' => 0,
                'member_id' => $member['memberId'],
                'assignments' => isset($members[$key]) ? $members[$key] : []
            ];
            $data[] = $item;
        }
		$this->assignments = $data;
    }

	public function getAssignments() : array
	{
		return $this->assignments;
	}
}
