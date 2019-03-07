<?php

namespace Agoat\RedisTransport\Transport;


use Agoat\RedisTransport\Serializer\RedisStreamSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;


class RedisTransportFactory implements TransportFactoryInterface
{
    private $debug;
    private $serializer;

    /**
     * RedisTransportFactory constructor.
     */
    public function __construct(SerializerInterface $serializer = null, bool $debug = false)
    {
        $this->serializer = $serializer ?? RedisStreamSerializer::create();
        $this->debug = $debug;
    }

    public function createTransport(string $dsn, array $options): TransportInterface
    {
        // Set timeout and read_timeout to the same value
        if (isset($options['timeout'])) {
            $options['read_timeout'] = $options['timeout'];
        }
        elseif (isset($options['read_timeout'])) {
            $options['timeout'] = $options['read_timeout'];
        }

        $channel = $options['channel'] ?? 'default';
        $group = $options['group'] ?? 'default';
        $consumer = $options['consumer'] ?? \gethostname();
        $namespace = $options['namespace'] ?? 'ns';
        $timeout = $options['timeout'] ?? 30;
        $count = $options['count'] ?? 1;

        return new RedisStreamTransport(\Redis::createConnection($dsn, $options), $namespace, $this->serializer, $channel, $group, $consumer, $timeout, $count);
    }

    public function supports(string $dsn, array $options): bool
    {
        return 0 === strpos($dsn, 'redis://');
    }
}