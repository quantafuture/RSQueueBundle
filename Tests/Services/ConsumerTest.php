<?php

/**
 * RSQueueBundle for Symfony2
 *
 * Marc Morera 2013
 */

namespace Mmoreram\RSQueueBundle\Tests\Services;

use Mmoreram\RSQueueBundle\Model\JobData;
use Mmoreram\RSQueueBundle\Services\Consumer;

/**
 * Tests Consumer class
 */
class ConsumerTest extends \PHPUnit_Framework_TestCase
{

    /**
     * Tests consume method
     */
    public function testConsume()
    {
        $queueAlias = 'alias';
        $queue = 'queue';
        $timeout = 0;
        $payload = array('engonga');

        $redis = $this
            ->createMock('\Redis');

        $redis
            ->expects($this->once())
            ->method('zrangebyscore')
            ->with($this->equalTo($queue), $this->equalTo($timeout))
            ->will($this->returnValue([json_encode($payload)]));

        $serializer = $this
            ->createMock('Mmoreram\RSQueueBundle\Serializer\JsonSerializer');

        $serializer
            ->expects($this->once())
            ->method('revert')
            ->with($this->equalTo(json_encode($payload)))
            ->will($this->returnValue($payload));

        $queueAliasResolver = $this
            ->getMockBuilder('Mmoreram\RSQueueBundle\Resolver\QueueAliasResolver')
            ->setMethods(array('getQueue', 'getQueueAlias'))
            ->disableOriginalConstructor()
            ->getMock();

        $queueAliasResolver
            ->expects($this->once())
            ->method('getQueue')
            ->with($this->equalTo($queueAlias))
            ->will($this->returnValue($queue));

        $queueAliasResolver
            ->expects($this->once())
            ->method('getQueueAlias')
            ->with($this->equalTo($queue))
            ->will($this->returnValue($queueAlias));

        $eventDispatcher = $this
            ->createMock('Symfony\Component\EventDispatcher\EventDispatcher', array('dispatch'));

        $eventDispatcher
            ->expects($this->once())
            ->method('dispatch');

        $consumer = new Consumer($eventDispatcher, $redis, $queueAliasResolver, $serializer);
        $job = $consumer->consume($queueAlias, $timeout);

        $this->assertInstanceOf(JobData::class, $job);
    }
}
