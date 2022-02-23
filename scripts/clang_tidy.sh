includes='-I../include -I../extern'
checks='abseil-*,bugprone-*,cert-*,clang-analyzer-*,concurrency-*,cppcoreguidelines-*,-cppcoreguidelines-pro-type-reinterpret-cast,google-*,misc-*,modernize-*'

# usage: ./clang_tidy.sh <source0> [... <sourceN>]
i=1;
j=$#;
if [ $j -eq 0 ]; then
    echo "usage: ./clang_tidy.sh <source0> [... <sourceN>]"
fi
while [ $i -le $j ] 
do
    clang-tidy-13 $1 -checks=$checks -- $includes 
    i=$((i + 1));
    shift 1;
done
