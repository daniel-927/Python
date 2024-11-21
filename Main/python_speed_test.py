import timeit
# 使用前 先进行编译 python setup.py build_ext --inplace
python = timeit.timeit('program2.fib(100000)',
                       setup='import program2', number=1)
cython = timeit.timeit('program1.fib(100000)',
                       setup='import program1', number=1)

print("Python Time: ", python)
print("Cython Time: ", cython)
print(f"{python / cython}x times faster")
