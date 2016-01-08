<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace KafkaTest;
use Kafka\Client;

/**
+------------------------------------------------------------------------------
 * Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
 *
 * @package
 * @version $_SWANBR_VERSION_$
 * @copyright Copyleft
 * @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
 */

class ClientTest extends \PHPUnit_Framework_TestCase
{
    // {{{ consts
    // }}}
    // {{{ members
    // }}}
    // {{{ functions
    // {{{ public function testGetHostByPartitionWhenPartitionDoesNotExist()

    /**
     * This tests getHostByPartition when the partition does not exist in the underlying metadata.
     * It's expected to throw an exception
     *
     * @access public
     * @return void
     */
    public function testGetHostByPartitionWhenPartitionDoesNotExist()
    {
        // Define our expected exception
        $this->setExpectedException('\Kafka\Exception');

        // Define our test inputs
        $topicName = 'myTopic';
        $partitionId = 12;

        // Define mock return values
        $returnedPartitionMetadata = null;

        // Mock our cluster metadata interface
        $mockClusterMetadata = $this->getMockBuilder('\Kafka\ClusterMetadata')->getMock();
        $mockClusterMetadata
            ->expects($this->exactly(1))
            ->method('getPartitionState')
            ->with($topicName, $partitionId)
            ->willReturn($returnedPartitionMetadata);
        $mockClusterMetadata
            ->expects($this->never())
            ->method('refreshMetadata');

        // Create instance of client with our mock
        $client = new Client($mockClusterMetadata);
        $client->getHostByPartition($topicName, $partitionId);
    }

    // }}}
    // {{{ public function testGetHostByPartitionWhenPartitionEverythingWorksFirstTime()

    /**
     * This tests getHostByPartition when the underlying metadata returns good broker and partition data.
     *
     * @access public
     * @return void
     */
    public function testGetHostByPartitionWhenPartitionEverythingWorksFirstTime()
    {
        // Define our test inputs
        $topicName = 'myTopic';
        $partitionId = 12;

        // Define mock return values
        $returnedPartitionMetadata = $this->createPartitionState(0, 2, array(1,2,3), array(1,2));
        $returnedBrokerList = array(
            1 => array(
                'host' => 'host1',
                'port' => 9092,
            ),
            2 => array(
                'host' => 'host2',
                'port' => 9092,
            ),
            3 => array(
                'host' => 'host3',
                'port' => 9092,
            ),
        );


        // Mock our cluster metadata interface
        $mockClusterMetadata = $this->getMockBuilder('\Kafka\ClusterMetadata')->getMock();
        $mockClusterMetadata
            ->expects($this->exactly(1))
            ->method('getPartitionState')
            ->with($topicName, $partitionId)
            ->willReturn($returnedPartitionMetadata);
        $mockClusterMetadata
            ->expects($this->exactly(1))
            ->method('listBrokers')
            ->willReturn($returnedBrokerList);
        $mockClusterMetadata
            ->expects($this->never())
            ->method('refreshMetadata');

        // Create instance of client with our mock
        $client = new Client($mockClusterMetadata);
        $result = $client->getHostByPartition($topicName, $partitionId);

        // We expect the result to be broker2 since it is the leader
        $this->assertEquals('host2:9092', $result, 'Expected broker2 host to be returned');
    }

    // }}}
    // {{{ public function testGetHostByPartitionWhenPartitionReturnsIncorrectLeaderWithRetry()

    /**
     * This tests getHostByPartition when the underlying metadata returns invalid partition leader
     * information on the first pass, but then after refreshing returns good data.
     *
     * @access public
     * @return void
     */
    public function testGetHostByPartitionWhenPartitionReturnsIncorrectLeaderWithRetry()
    {
        // Define our test inputs
        $topicName = 'myTopic';
        $partitionId = 12;

        // Define mock return values
        // This says that broker2 is the leader
        $firstReturnedPartitionMetadata = $this->createPartitionState(0, 2, array(1,2,3), array(1,2,3));

        // This one says that broker1 is now the leader
        $secondReturnedPartitionMetadata = $this->createPartitionState(0, 1, array(1,3), array(1,3));

        // This is missing broker id 2
        $returnedBrokerList = array(
            1 => array(
                'host' => 'host1',
                'port' => 9092,
            ),
            3 => array(
                'host' => 'host3',
                'port' => 9092,
            ),
        );

        // Mock our cluster metadata interface
        $mockClusterMetadata = $this->getMockBuilder('\Kafka\ClusterMetadata')->getMock();
        $mockClusterMetadata
            ->expects($this->exactly(2))
            ->method('getPartitionState')
            ->with($topicName, $partitionId)
            ->will($this->onConsecutiveCalls($firstReturnedPartitionMetadata, $secondReturnedPartitionMetadata));
        $mockClusterMetadata
            ->expects($this->exactly(2))
            ->method('listBrokers')
            ->willReturn($returnedBrokerList);
        $mockClusterMetadata
            ->expects($this->exactly(1))
            ->method('refreshMetadata');

        // Create instance of client with our mock
        $client = new Client($mockClusterMetadata);
        $result = $client->getHostByPartition($topicName, $partitionId);

        // We expect the result to be broker1 since it is the leader
        $this->assertEquals('host1:9092', $result, 'Expected broker1 host to be returned');
    }

    // }}}
    // {{{ public function testGetHostByPartitionWhenPartitionReturnsIncorrectBrokerListWithRetry()

    /**
     * This tests getHostByPartition when the underlying metadata returns invalid broker
     * information on the first pass, but then after refreshing returns good data.
     *
     * @access public
     * @return void
     */
    public function testGetHostByPartitionWhenPartitionReturnsIncorrectBrokerListWithRetry()
    {
        // Define our test inputs
        $topicName = 'myTopic';
        $partitionId = 12;

        // Define mock return values
        // This says that broker2 is the leader
        $returnedPartitionMetadata = $this->createPartitionState(0, 2, array(1,2,3), array(1,2,3));

        // This is missing broker id 2
        $firstReturnedBrokerList = array(
            1 => array(
                'host' => 'host1',
                'port' => 9092,
            ),
            3 => array(
                'host' => 'host3',
                'port' => 9092,
            ),
        );

        // Has brokerId 2
        $secondReturnedBrokerList = array(
            1 => array(
                'host' => 'host1',
                'port' => 9092,
            ),
            2 => array(
                'host' => 'host2',
                'port' => 9092,
            ),
            3 => array(
                'host' => 'host3',
                'port' => 9092,
            ),
        );

        // Mock our cluster metadata interface
        $mockClusterMetadata = $this->getMockBuilder('\Kafka\ClusterMetadata')->getMock();
        $mockClusterMetadata
            ->expects($this->exactly(2))
            ->method('getPartitionState')
            ->with($topicName, $partitionId)
            ->willReturn($returnedPartitionMetadata);
        $mockClusterMetadata
            ->expects($this->exactly(2))
            ->method('listBrokers')
            ->will($this->onConsecutiveCalls($firstReturnedBrokerList, $secondReturnedBrokerList));
        $mockClusterMetadata
            ->expects($this->exactly(1))
            ->method('refreshMetadata');

        // Create instance of client with our mock
        $client = new Client($mockClusterMetadata);
        $result = $client->getHostByPartition($topicName, $partitionId);

        // We expect the result to be broker2 since it is the leader
        $this->assertEquals('host2:9092', $result, 'Expected broker2 host to be returned');
    }

    // }}}
    // {{{ public function testGetHostByPartitionWhenPartitionWithRetryFails()

    /**
     * This tests getHostByPartition when the underlying metadata returns missing broker information
     * on the first and second pass, expected to throw an exception
     *
     * @access public
     * @return void
     */
    public function testGetHostByPartitionWhenPartitionWithRetryFails()
    {
        // Define our expected exception
        $this->setExpectedException('\Kafka\Exception');

        // Define our test inputs
        $topicName = 'myTopic';
        $partitionId = 12;

        // Define mock return values
        // This says that broker2 is the leader
        $returnedPartitionMetadata = $this->createPartitionState(0, 2, array(1,2,3), array(1,2,3));

        // This is missing broker id 2
        $returnedBrokerList = array(
            1 => array(
                'host' => 'host1',
                'port' => 9092,
            ),
            3 => array(
                'host' => 'host3',
                'port' => 9092,
            ),
        );

        // Mock our cluster metadata interface
        $mockClusterMetadata = $this->getMockBuilder('\Kafka\ClusterMetadata')->getMock();
        $mockClusterMetadata
            ->expects($this->exactly(2))
            ->method('getPartitionState')
            ->with($topicName, $partitionId)
            ->willReturn($returnedPartitionMetadata);
        $mockClusterMetadata
            ->expects($this->exactly(2))
            ->method('listBrokers')
            ->willReturn($returnedBrokerList);
        $mockClusterMetadata
            ->expects($this->exactly(1))
            ->method('refreshMetadata');

        // Create instance of client with our mock
        $client = new Client($mockClusterMetadata);
        $client->getHostByPartition($topicName, $partitionId);
    }

    // }}}
    // {{{ private function createPartitionState()

    /**
     * Simple helper method to generate Partition state metadata.
     *
     * @param int $errorCode
     * @param int $leader
     * @param int[] $replicas
     * @param int[] $isr
     * @return array
     */
    private function createPartitionState($errorCode, $leader, $replicas, $isr)
    {
        return array(
            'errCode'  => $errorCode,
            'leader'   => $leader,
            'replicas' => $replicas,
            'isr'      => $isr,
        );
    }

    // }}}
    // }}}
}
