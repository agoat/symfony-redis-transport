<?php


namespace Agoat\RedisTransport\Stamp;


use Symfony\Component\Messenger\Stamp\StampInterface;

class HighPriorityStamp implements StampInterface
{
    private const PRIORITY = 'high';

    public function getPriority()
    {
        return self::PRIORITY;
    }

}