#!/bin/bash
#Script to download discussions from disqus

for file in $(ls /bos/tmp19/spalakod/clueweb12pp/disqus/*-thread-ids.threads);
do
    nohup lein trampoline run -d $file &
done
