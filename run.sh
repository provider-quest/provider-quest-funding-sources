#! /bin/bash

set -e

TZ=UTC node find-miners-and-funders.mjs --delete $1
