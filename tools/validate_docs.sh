#!/bin/bash

set -e

# NOTE(sileht): The flags -W with sphinx-versionning does not return 1
# but when a build fail and the flags is present, the failed version does not appear
# in the version selector. This testchecks this.
ret=0
for path in doc/build/html/stable*; do
    version=$(basename $path)  # stable_XXX
    if ! grep -q $version doc/build/html/index.html ; then
        echo "Version $version is missing"
        ret=1
    fi
done
exit $ret
