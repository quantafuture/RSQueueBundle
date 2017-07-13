<?php

namespace Mmoreram\RSQueueBundle\Listeners;

use Doctrine\Bundle\DoctrineBundle\Registry;
use Mmoreram\RSQueueBundle\Event\RSQueueConsumerEvent;
use Doctrine\Common\Persistence\ManagerRegistry;

/**
 * Class ConsumerListener
 *
 * @package Mmoreram\RSQueueBundle\Listeners
 */
class ConsumerListener
{
    /**
     * @var Registry
     */
    protected $registry;

    /**
     * ConsumerListener constructor.
     *
     * @param ManagerRegistry|null $registry
     */
    public function __construct(ManagerRegistry $registry = null)
    {
        $this->registry = $registry;
    }

    /**
     * @param RSQueueConsumerEvent $event
     */
    public function checkRSQConsumerEvent(RSQueueConsumerEvent $event)
    {
        if ($this->registry instanceof ManagerRegistry) {
            $allManagers = $this->registry->getManagers();
            foreach ($allManagers as $manager) {
                $manager->clear();
            }
        }
    }
}
