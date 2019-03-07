<?php

namespace Agoat\RedisTransport\MessageHandler;


use Agoat\RedisTransport\Message\NullMessage;
use Symfony\Component\Messenger\Handler\MessageHandlerInterface;

class NullMessageHandler implements MessageHandlerInterface
{
    public function __invoke(NullMessage $message)
    {
    }
}