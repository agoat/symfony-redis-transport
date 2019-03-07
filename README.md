# Symfony messenger transport with redis


### Initialization

Add a `redis_transport.yaml` into the `config/packages` directory with the following content:

```yaml
services:
    _defaults:
        autowire: true
        autoconfigure: true
        public: false

    Agoat\RedisTransport\:
        resource: '../../vendor/agoat/redis-transport/src/*'
```


### Messenger configuration 

```yaml
framework:
    messenger:
        transports:
            # Addd a redis transport with (optional) options
            redis:
                dsn: '%redis.transport.dsn%'
                options:
                    namespace: 'myOwnNamespace' # A custom namespace
                    channel: 'myChannel' # A channel name to split communication
                    group: 'myGroup' # The groupname for multiple concurrent worker
                    timeout: 60 # default
                    count: 10 # default          
```
