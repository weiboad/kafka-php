FROM php:7.1-alpine

WORKDIR /opt/kafka-php

CMD ["./vendor/bin/phpunit", "--testsuite", "functional"]
