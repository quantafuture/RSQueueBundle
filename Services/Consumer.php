<?php

/**
 * RSQueueBundle for Symfony2
 *
 * Marc Morera 2013
 */

namespace Mmoreram\RSQueueBundle\Services;

use Mmoreram\RSQueueBundle\Model\AbstractJobData;
use Mmoreram\RSQueueBundle\Model\JobData;
use Mmoreram\RSQueueBundle\Model\NoJobData;
use Mmoreram\RSQueueBundle\Services\Abstracts\AbstractService;
use Mmoreram\RSQueueBundle\RSQueueEvents;
use Mmoreram\RSQueueBundle\Exception\InvalidAliasException;
use Mmoreram\RSQueueBundle\Event\RSQueueConsumerEvent;

/**
 * Consumer class
 *
 * This class
 */
class Consumer extends AbstractService
{

    /**
     * Retrieve queue value, with a defined timeout
     *
     * This method accepts a single queue alias or an array with alias
     * Every new element will be popped from one of defined queue
     *
     * Also, new Consumer event is triggered everytime a new element is popped
     *
     * @param Mixed $queueAlias Alias of queue to consume from ( Can be an array of alias )
     *
     * @return array
     *
     * @throws InvalidAliasException If any alias is not defined
     */
    public function consume($queueAlias)
    {
        $queues = is_array($queueAlias)
            ? $this->queueAliasResolver->getQueues($queueAlias)
            : [$this->queueAliasResolver->getQueue($queueAlias)];

        $payloads = [];

        foreach ($queues as $queue) {
            $jobs = $this->redis->zrangebyscore($queue, 0, time(), ['limit' => [0, 1000]]);

            if (!is_array($jobs)) {
                continue;
            }

            foreach ($jobs as $job) {
                $this->redis->zRem($queue, $job);

                $payload                    = $this->serializer->revert($job);
                $givenQueueAlias            = $this->queueAliasResolver->getQueueAlias($queue);

                if (!isset($payloads[$givenQueueAlias])) {
                    $payloads[$givenQueueAlias] = [];
                }

                $payloads[$givenQueueAlias][] = $payload;

                /**
                 * Dispatching consumer event...
                 */
                $consumerEvent = new RSQueueConsumerEvent($payload, $job, $givenQueueAlias, $queue, $this->redis);
                $this->eventDispatcher->dispatch(RSQueueEvents::RSQUEUE_CONSUMER, $consumerEvent);
            }
        }

        return $payloads;
    }
}
