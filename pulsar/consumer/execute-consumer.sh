
/mnt/d/pulsar/apache-pulsar-5.0.0-M1/bin/pulsar-admin topics delete \
  --force \
  persistent://public/default/demo

/mnt/d/pulsar/apache-pulsar-5.0.0-M1/bin/pulsar-admin topics create \
  persistent://public/default/demo

go run consumer.go
