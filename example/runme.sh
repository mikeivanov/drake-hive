#!/bin/bash
set -e
drake --plugins=plugins.edn --workflow=main.drake "$@"
