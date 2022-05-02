#!/bin/bash
echo "Setting Up The Path"
export MY_INSTALL_DIR=$HOME/.grpc
export PATH="$MY_INSTALL_DIR/bin:$PATH"
export PKG_CONFIG_PATH=$MY_INSTALL_DIR/lib/pkgconfig/
export PKG_CONFIG_PATH=$MY_INSTALL_DIR/lib64/pkgconfig:$PKG_CONFIG_PATH
