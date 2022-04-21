import math

a = {
    "a":1,
    "b":1,
    "c":-1,
    "d":2,
    "e":2,
    }

def biggestMatch(a):
        quantities = {}
        for value in a.values():
            quantities.setdefault(value,0)
            quantities[value] += 1
        reversed_list = sorted(quantities.items(),key=lambda x:-x[0])
        majority = math.floor(len(a.keys())/2)+1
        sum = 0
        for ele in reversed_list:
            sum += ele[1]
            if sum >= majority:
                return ele[0]

print(biggestMatch(a))