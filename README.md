drake-hive
==========

A Hive plugin for [Drake](https://github.com/Factual/drake).

## Features

###  Filesystem

    dump.tsv <- !hive:/default/foobar
        hive -e "SELECT * FROM foobar" > $[OUTPUT]

### Protocol

    !hive:/default/foobar <- !hive:/default/sample_07 [hive]
        CREATE TABLE IF NOT EXISTS ${env:TABLEOUT} LIKE ${env:TABLEIN};
        INSERT OVERWRITE TABLE ${env:TABLEOUT} SELECT * FROM ${env:TABLEIN};

### Usage

1. Put example/plugins.edn alongside with your Drakefile.
2. Run Drake: `drake --plugins=plugins.edn ...`

