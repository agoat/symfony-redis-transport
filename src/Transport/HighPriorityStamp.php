<?php


namespace Agoat\RedisTransport\Transport;


use Symfony\Component\Messenger\Stamp\StampInterface;

class HighPriorityStamp implements StampInterface
{
    private const PRIORITY = 'high';

    public function getPriority()
    {
        return self::PRIORITY;
    }

}