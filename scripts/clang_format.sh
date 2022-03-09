find ../ -iname *.cpp -o -iname *.hpp -not -path "*/extern/*" | xargs -I '{}' clang-format-10 -i '{}'
