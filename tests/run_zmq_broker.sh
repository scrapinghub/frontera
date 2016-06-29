#!/usr/bin/env bash
# Start ZMQ broker with hostname instead of IP
python -m frontera.contrib.messagebus.zeromq.broker --address localhost --port '5580' 2>> broker.log &

# Start ZMQ broker with IPv6
python -m frontera.contrib.messagebus.zeromq.broker --address '::1' --port '5570' 2>> broker.log &

# Start ZMQ broker with wildcard
python -m frontera.contrib.messagebus.zeromq.broker --address '*' --port '5560' 2>> broker.log &

# Start ZMQ broker with default settings
python -m frontera.contrib.messagebus.zeromq.broker 2>> broker.log &
