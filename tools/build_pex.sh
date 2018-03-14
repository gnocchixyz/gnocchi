#!/bin/bash

set -o pipefail
set -e
set -x

rm -rf $CACHE_DIR/gnocchi-* gnocchi.egg-info

if [ "$1" == "doc" ]; then
    # NOTE(sileht): For binary generation pex need setuptools<34 for the doc
    # generation (but not for other feature that why it's not in its
    # requirements), so we downgrade it
    pip install -U "setuptools>=30.3,<34"

    mkdir -p doc/source/dist
    pex $(pwd)[$EXTRAS] -v --cache-dir=$CACHE_DIR -m gnocchi.pex -o doc/source/dist/$BINARY
    chmod +x doc/source/dist/$BINARY
    sed -e "s/##BINARY##/$BINARY/g" doc/source/install-pex.rst.tmpl > doc/source/install-pex.rst

    # TODO(sileht): Generate a binary with the last tarball of pypi when pex
    # code land in a stable branch
else
    python -c "from gnocchi import pex; pex.build_pex_binary('dist')"
fi
