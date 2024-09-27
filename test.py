from importlib.metadata import version

print(version("inbound"))

foo = 2

if foo is None or 3:
    print("bar")


if 3:
    print("bar")
