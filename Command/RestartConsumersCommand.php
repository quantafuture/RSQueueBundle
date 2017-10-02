<?php

namespace Mmoreram\RSQueueBundle\Command;

use Mmoreram\RSQueueBundle\Command\Abstracts\AbstractExtendedCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Process\Process;

/**
 * Class RestartConsumersCommand
 *
 * @package Mmoreram\RSQueueBundle\Command
 */
class RestartConsumersCommand extends AbstractExtendedCommand
{
    const RSQUEUE_CONSUMER_PIDS_KEY = 'rsqueue_consumer_pids_key';

    /**
     * @var \Redis
     */
    protected $redis;

    /**
     * @var string
     */
    protected $namespace;

    /**
     * @param \Redis $redis
     */
    public function __construct(\Redis $redis, $namespace)
    {
        $this->redis = $redis;
        $this->namespace = $namespace;

        parent::__construct();
    }

    /**
     * @return int
     */
    protected function stopExecute()
    {
        return 0;
    }

    /**
     * Configure
     */
    protected function configure()
    {
        $this
            ->setName('rsqueue:restart-consumers')
            ->setDescription('Restart consumers.');

        parent::configure();
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     *
     * @return void
     */
    protected function executeCommand(InputInterface $input, OutputInterface $output)
    {
        $keys = $this->redis->keys(self::RSQUEUE_CONSUMER_PIDS_KEY.'_'.$this->namespace.'_*');
        $pids = [];

        foreach ($keys as $key) {
            $pid = intval($this->redis->get($key));
            posix_kill($pid, SIGTERM);
            $pids[] = $pid;
        }

        $waitProcess = new Process(sprintf('wait %s', implode(' ', $pids)));
        $waitProcess->run();

        return;
    }
}
