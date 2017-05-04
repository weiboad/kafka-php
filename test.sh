#!/bin/bash
rm -rf build/logs/*
./tests/run-tests.php -c ../build/logs/clover. all
./vendor/bin/coveralls --dry-run -v

