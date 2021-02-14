#!/bin/bash

cd $@

cargo +nightly test
