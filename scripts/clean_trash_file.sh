#!/bin/bash

find ../ -name erl_crash.dump | xargs rm -f 
find ../ -name '*~' | xargs rm -f
find ../ -name '*#' | xargs rm -f
