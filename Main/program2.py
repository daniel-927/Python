def fib(n):
    n1, n2 = 0, 1

    for i in range(1, n + 1):
        temp = n1 + n2
        n1 = n2
        n2 = temp
    return n2
