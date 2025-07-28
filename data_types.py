"""
int, str, float, set, dict, list, tuple
"""

import pandas as pd

df = pd.read_excel("simple.xlsx")
df1 = df.to_csv("simple.csv")
print(df1)

a = set()
# a.add('as')
a.add([1, 2, 3])
print(a)

class User:
    users = []

    def __init__(self, name):
        self.name = name
        User.users.append(name)

    @classmethod
    def get_user_count(cls):
        return len(cls.users)

print(User.get_user_count())  # 0
u1 = User("Kamron")
u2 = User("Ali")
print(User.get_user_count())  # 2


class MathHelper:
    @staticmethod
    def add(a, b):
        return a + b

print(MathHelper.add(3, 5))  # 8



from abc import ABC, abstractmethod

class Animal(ABC):
    @abstractmethod
    def sound(self):
        pass

class Dog(Animal):
    def sound(self):
        return "Woof!"

# animal = Animal()       # ❌ Error: abstract class
dog = Dog()
print(dog.sound())        # ✅ "Woof!"
