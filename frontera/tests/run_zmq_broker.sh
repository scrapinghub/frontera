#!/usr/bin/env bash
# Start ZMQ broker with hostname instead of IP
python -m frontera.contrib.messagebus.zeromq.broker --address localhost 2>> broker.log &
sleep 5
kill $(ps aux | grep 'python -m frontera.contrib.messagebus.zeromq.broker' | awk '{print $2}')

# Start ZMQ broker with IPv6
python -m frontera.contrib.messagebus.zeromq.broker --address '::1' 2>> broker.log &
sleep 5
kill $(ps aux | grep 'python -m frontera.contrib.messagebus.zeromq.broker' | awk '{print $2}')

# Start ZMQ broker with wildcard
python -m frontera.contrib.messagebus.zeromq.broker --address '*' 2>> broker.log &
sleep 5
kill $(ps aux | grep 'python -m frontera.contrib.messagebus.zeromq.broker' | awk '{print $2}')

# Start ZMQ broker with default settings
python -m frontera.contrib.messagebus.zeromq.broker 2>> broker.log &
