#!/bin/bash

src_dirs=( benchmark engine examples include tests )
file_types=( h hpp c cpp )

for dir in "${src_dirs[@]}"
do
    for type in "${file_types[@]}"
    do
        find "../$dir/" -iname "*.$type" | xargs -I '{}' clang-format-9 -i '{}'
    done
done
