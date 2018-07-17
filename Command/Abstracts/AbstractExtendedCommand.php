<?php

namespace Mmoreram\RSQueueBundle\Command\Abstracts;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Class AbstractExtendedCommand
 *
 * @package Mmoreram\RSQueueBundle\Command\Abstracts
 */
abstract class AbstractExtendedCommand extends ContainerAwareCommand
{
    /**
     * Important to always call parent::configure!!!!!
     */
    protected function configure()
    {
        $this
            ->addOption(
                'gracefulShutdown',
                false,
                InputOption::VALUE_OPTIONAL,
                'Force graceful shutdown of command.'
            );
    }

    /**
     * @param InputInterface  $input
     * @param OutputInterface $output
     *
     * @return int | null
     */
    final protected function execute(InputInterface $input, OutputInterface $output)
    {
        $graceful = $input->getOption('gracefulShutdown');

        if ($graceful) {
            pcntl_signal(SIGTERM, [$this, 'stopExecute']);
            pcntl_signal(SIGINT, [$this, 'stopExecute']);
        }

        $this->executeCommand($input, $output);

        return 0;
    }

    /**
     * Definition of what will happen when you kill the command
     *
     * @return void
     */
    abstract protected function stopExecute();

    /**
     * Definition of the command
     *
     * @param InputInterface  $input
     * @param OutputInterface $output
     *
     * @return int | null
     */
    abstract protected function executeCommand(InputInterface $input, OutputInterface $output);
}
