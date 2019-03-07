<?php


namespace Agoat\RedisTransport\Transport;


use Agoat\RedisTransport\Message\NullMessage;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class RedisStreamReceiver implements ReceiverInterface
{
    private const PRIORITISATION = ['high', 'default'];

    private const MAX_STREAM_LENGTH = 1000000;

    private const MIN_IDLE_TIME = 600000; // 10 min (in microseconds)
    private const MIN_OBSERVATION_INTERVAL = 60; // 1 min (in seconds)

    private $redis;
    private $serializer;
    private $shouldStop;
    private $key;
    private $group;
    private $consumer;
    private $timeout;
    private $count;
    private $backlog = true;

    public function __construct(\Redis $redis, SerializerInterface $serializer, string $key, string $group, string $consumer, int $timeout, int $count)
    {
        $this->redis = $redis;
        $this->serializer = $serializer;
        $this->key = $key;
        $this->group = $group;
        $this->consumer = $consumer;
        $this->timeout = $timeout;
        $this->count = $count;
    }

    /**
     * @param callable $handler
     * @throws \Throwable
     */
    public function receive(callable $handler): void
    {
        foreach (self::PRIORITISATION as $priority) {
            $streams[] = $this->key . ':' . $priority;

            // Init stream group(s)
            if (! $this->redis->xinfo('CONSUMERS', $this->key . ':' . $priority, $this->group)) {
                $this->redis->xadd($this->key . ':' . $priority, '*', $this->serializer->encode(new Envelope(new NullMessage()))); // TODO Remove if MKSTREAM is available in next PHPREDIS release
                $this->redis->xGroup('CREATE', $this->key . ':' . $priority, $this->group, '$');
            }
        }

        if (! $this->redis->get($this->key . ':cron')) {
            $this->redis->set($this->key . ':cron', microtime(true));
        }

        while (! $this->shouldStop) {
            $this->listenToStreams($streams, $handler);

            if ($this->redis->get($this->key . ':cron') + self::MIN_OBSERVATION_INTERVAL < microtime(true)) {
                $this->redis->set($this->key . ':cron', microtime(true));

                $this->scheduleMessages();
                $this->claimMessages($streams, $handler);
                $this->garbageCollection($streams);
            }
        }
    }

    private function scheduleMessages()
    {
        if (
            ! empty($scheduledMessageIds = $this->redis->zRangeByScore($this->key . ':schedule', 0, microtime(true))) &&
            (! ($lock = $this->redis->get($this->key . ':schedule:lock')) || $lock + self::MIN_OBSERVATION_INTERVAL < microtime(true))
        ) {
            $this->redis->set($this->key . ':schedule:lock', microtime(true));

            foreach ($scheduledMessageIds as $messageId) {
                $encodedMessage = $this->redis->hGetAll($this->key . ':data:' . $messageId);
                $priority = empty(array_filter(array_keys($encodedMessage), function($key) {
                    return stripos($key, 'HighPriorityStamp') !== FALSE;
                })) ? 'default': 'high';

                // TODO This should be done in a atomic operation (via lua script)
                $this->redis->multi();
                $this->redis->xAdd($this->key . ':' . $priority, '*', $encodedMessage);
                $this->redis->zDelete($this->key . ':schedule', $messageId);
                $this->redis->Del($this->key . ':data:' . $messageId);
                $this->redis->exec();
            }

            $this->redis->del($this->key . ':schedule:lock');

            // TODO Use logger
            echo '[' . (new \DateTimeImmutable())->format('Y-m-d H:i:s') . '] Queued scheduled messages' . PHP_EOL;
        }
    }

    private function claimMessages(array $streams, callable $handler)
    {
        foreach ($streams as $key) {
            foreach ($this->redis->xinfo('CONSUMERS', $key, $this->group) as $consumer) {
                $name = $consumer[1];
                $pending = $consumer[3];
                $idle = $consumer[5];
                if ($idle > self::MIN_IDLE_TIME) { // idle
                    if ($pending > 0) { // pending
                        $pendingMessages = $this->redis->xPending($key, $this->group, '-', '+', $this->count * 2, $name);

                        if (!empty($pendingMessages)) {
                            // TODO Use logger
                            echo '[' . (new \DateTimeImmutable())->format('Y-m-d H:i:s') . '] Claimed messages from ' . $name . PHP_EOL;

                            $this->handleMessages([$key => $this->redis->xClaim($key, $this->group, $this->consumer, self::MIN_IDLE_TIME, array_map(function ($message) {
                                return $message[0];
                            }, $pendingMessages))], $handler);
                        }
                    } else {
                        $this->redis->xGroup('DELCONSUMER', $key, $this->group, $name);
                    }

                    continue 2; // Handle only one dead consumer at a time
                }
            }
        }
    }

    private function garbageCollection(array $streams)
    {
        foreach ($streams as $key) {
            $this->redis->xTrim($key, self::MAX_STREAM_LENGTH, true);
        }
    }

    /**
     * @param array $streams
     * @param callable $handler
     * @throws \Throwable
     */
    private function listenToStreams(array $streams, callable $handler)
    {
        try {
            $streamMessages = $this->redis->xReadGroup($this->group, $this->consumer, array_fill_keys($streams, $this->backlog ? '0' : '>'), $this->count, $this->timeout * rand(800, 990));
            $this->handleMessages($streamMessages, $handler);
        } catch (\RedisException $e) {
            dump($e);
            // just ignore and retry
        } catch (\Throwable $e) {
            dump($e);
            throw $e;
        } finally {
            if (\function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }
        }
    }

    private function handleMessages(array $streamMessages, callable $handler)
    {
        $emptyStreamCount = 0;
        foreach ($streamMessages as $key => $messages) {
            if (empty($messages)) {
                $emptyStreamCount++;
                continue;
            }

            foreach ($messages as $id => $message) {
                if(class_exists($message['X-Message-Type'])) { // Only handle known message classes
                    $handler($this->serializer->decode($message));
                } else {
                    echo 'Unknown message class \'' . $message['X-Message-Type'] . '\'' . PHP_EOL;
                }

                $this->redis->xAck($key, $this->group, [$id]);
            }
        }

        if ($this->backlog && $emptyStreamCount == count($streamMessages)) {
            $this->backlog = false;
        }
    }

    public function stop(): void
    {
        $this->shouldStop = true;
    }

}