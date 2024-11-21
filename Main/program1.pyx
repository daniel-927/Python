cpdef int fib(int n):
    cdef int n1 = 0
    cdef int n2 = 1
    cdef int temp, i

    for i in range(1, n + 1):
        temp = n1 + n2
        n1 = n2
        n2 = temp
    return n2
