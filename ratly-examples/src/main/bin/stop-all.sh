#!/usr/bin/env bash


 kill $(jps | grep 'RatlyRunner' | grep -v 'grep' | awk '{print $1}')
 echo "All Ratly examples have been stopped."
