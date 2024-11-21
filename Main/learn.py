# learn-day01
# 交叉赋值
# x, y, z = 3, 4, 5
# z, y, x = x, y, z
# print(x, y, z)
#
# score = input("please enter:")
# score = int(score)
#
# if score >= 90:
#     print("优秀")
# elif score >= 80:
#     print("良好")
# elif score >= 70:
#     print("一般")
# else:
#     print("all is sb")

age = 22
is_beautiful = True
star = '天平座'

if 16 < age < 20 and is_beautiful and star == '天平座':
    print("开始表白....")
    is_beautiful = True
    if is_beautiful:
        print('两个人从此过上了没羞没臊的生活。')
else:
    print('阿姨好，打扰了！')

info = [
    ['name', 'daniel'],
    ['age', '18'],
    ['gender', 'male']
]
d = {}
for k, v in info:
    d[k] = v
print(d)

c = {}
c['zxc'] = 'yyy'
print(c)

res = dict(info)
print(res)

keys = ['name', 'age', 'gender']
z = {}.fromkeys(keys, None)
print(z)


def func(x, y, *args):
    print(x, y, args)


func(1, *[2, 3, 4, 5, 6, 7])


def func2(**kwargs):
    print(kwargs)


func2(**{'x': 1, 'y': 2, 'z': 3})


def index(*args):
    print('index=>>>', args)


def wrapper(*args):
    index(args)


wrapper(1, 2, 3, 4)


def xyz(x, y, z, a, b, c):
    print(x)
    print(y)
    print(z)
    print(a)
    print(b)
    print(c)


xyz(111, *[333, 444], **{'b': 555, 'c': 666}, a=222)

xyz = 111111


def xyzxx():
    # xyz=222222
    print(xyz)


xyzxx()
print(xyz)

zzz = 1234


def foo1():
    print(zzz)


def foo2():
    zzz = 4321
    foo1()

    def foo3():
        print(zzz)

    foo3()


foo2()


# 三元表达式
x = 22
y = 77
z = x if x > y else y
print(z)


print('are you ok?')
