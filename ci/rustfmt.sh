#!/bin/bash

cargo fmt -- --check --config-path <(echo 'license_template_path = "HEADER"')
