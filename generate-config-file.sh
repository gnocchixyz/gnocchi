#!/bin/sh
mkdir -p etc/gnocchi
oslo-config-generator --output-file etc/gnocchi/gnocchi.conf \
                      --namespace gnocchi \
                      --namespace oslo.db \
                      --namespace oslo.log \
                      --namespace oslo.policy \
                      --namespace keystonemiddleware.auth_token
