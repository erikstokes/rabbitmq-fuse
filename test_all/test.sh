#!/bin/bash

if ! podman image exists rabbitmq-fuse
then
  (
  cd ..
  podman build -t rabbitmq-fuse .
  )
fi

podman rm -f rabbit-server
podman rm -f fuse-test

# recreate certs if they're not there
if [ ! -d ./tls-gen ]
then
  (
  git clone https://github.com/rabbitmq/tls-gen.git
  cd tls-gen/basic
  make CN=rabbit
  make CN=rabbit alias-leaf-artifacts
)
fi

# start rabbit container
podman run \
    --rm -d \
    --name rabbit-server \
    --hostname rabbit-server \
    -v ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:U \
    -v ./definitions.json:/etc/rabbitmq/definitions.json:U \
    -v ./enabled_plugins:/etc/rabbitmq/enabled_plugins:U \
    -v ./tls-gen/basic/result/ca_certificate.pem:/var/certs/ca_certificate.pem:U \
    -v ./tls-gen/basic/result/server_certificate.pem:/var/certs/server_certificate.pem:U \
    -v ./tls-gen/basic/result/server_key.pem:/var/certs/server_key.pem:U \
    -p 5671:5671 \
    -p 15672:15672 \
    -p 15692:15692 \
    -u 999:999 \
    rabbitmq:3.7.12-management
    
# wait for rabbit to be listening
while true
do
  echo Q | openssl s_client -showcerts -connect localhost:5671 -cert ./tls-gen/basic/result/client_rabbit_certificate.pem -key ./tls-gen/basic/result/client_rabbit_key.pem
  if [ $? -eq 0 ]
  then
    echo "connected!"
    break
  else
    echo "not connected"
    sleep 1
  fi
done

# start rabbitmq-fuse test container
podman run \
    --privileged \
    --rm -d \
    --name fuse-test \
    --network host \
    -v ./tls-gen/basic/result:/certs \
    -e RUST_LOG=debug \
    rabbitmq-fuse \
      ./target/debug/rabbitmq-fuse \
        --rabbit-addr 'amqp://localhost:5671/%2f?auth_mechanism=external' \
        --cert /certs/ca_certificate.pem \
        --key /certs/client_rabbit_key.p12 \
        --exchange logs \
        --publish-in=header \
        --max-unconfirmed=100000 \
        /mnt

echo "Fuse mount started"
        
# exec into the fuse-test container and log a message
podman exec -it fuse-test /bin/bash <<'EOF'
mkdir /mnt/stuff
echo '{"testmessagekey": "testmessagevalue"}' > /mnt/stuff/bla.jog
exit
EOF

# check for message
if curl -u admin:password -X POST http://localhost:15672/api/queues/%2F/logs/get --data '{"count":1,"ackmode":"ack_requeue_true","encoding":"auto"}' | grep "testmessagevalue"
then
  echo "PASS"
  podman rm -f rabbit-server
  podman rm -f fuse-test
else
  echo "FAIL"
fi

