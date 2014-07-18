#!/bin/bash
# Generate unique thread ids list

data_dir="/bos/tmp19/spalakod/clueweb12pp/disqus/"
script_home="/bos/usr0/spalakod/Documents/clueweb12pp/clueweb_disqus/"

cd $data_dir;
cat *-disqus-threads*.threads | sort | uniq > all.threads
cat *thread-ids*.downloaded | sort | uniq >> all.downloaded

cd $script_home;
lein trampoline run -t
