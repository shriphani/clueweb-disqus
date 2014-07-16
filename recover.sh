#!/bin/bash
# Script to read the recovery file and
# restart the crawl from there

lein trampoline run -b

recovery_file="/bos/tmp19/spalakod/clueweb12pp/disqus/recover.clj"

for line in $(cat $recovery_file);
do
    nohup lein trampoline run -r $line &
done
mv $recovery_file recovered-$(date +%s)
