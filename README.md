# rabbitdump

CLI utility to dump the contents of all stream data on a RabbitMQ instance.

## Installation

```sh
cargo install --git https://github.com/sbruton/rabbitdump
```

## Usage

```sh
Usage: rabbitdump [OPTIONS] <--json>

Options:
  -a, --amqp-host <AMQP_HOST>                  [default: localhost]
  -p, --amqp-port <AMQP_PORT>                  [default: 5672]
  -m, --amqp-mgmt-port <AMQP_MANAGEMENT_PORT>  [default: 15672]
  -u, --amqp-user <AMQP_USER>                  [default: guest]
  -P, --amqp-password <AMQP_PASSWORD>          [default: guest]
      --prefetch <PREFETCH_COUNT>              [default: 1000]
      --json
  -h, --help                                   Print help
```