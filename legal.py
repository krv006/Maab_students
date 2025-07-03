"""
1.	Son juft yoki toq ekanligini aniqlash:
    Foydalanuvchidan son kiritishni so‘rang va uning juft yoki toq ekanligini aniqlaydigan dastur yozing.

2. 2 ta list ga juft toq sonlardi bolib chiqish (1-100)

3.	Son juft yoki toq ekanligini aniqlash:
	Foydalanuvchidan son kiritishni so‘rang va uning juft yoki toq ekanligini aniqlaydigan dastur yozing.

4.	Fayl o‘qish va yozish:
	data.txt nomli fayl yarating va unga Hello, Python! matnini yozing.
	Keyin fayldan matnni o‘qing va ekranga chiqaring.

5.	Fibonacci sonlari:
	n sonini foydalanuvchidan so‘rab, n-gacha bo‘lgan Fibonacci sonlarini chiqaradigan funksiya yozing.

6.	List va Tuple ishlatish:
	Berilgan listdagi eng katta va eng kichik elementlarni topadigan funksiya yozing

"""


# task1
num = int(input("Enter a number:"))
if num % 2 ==1:
    print("odd number")
elif num % 2 ==0:
    print("even number")
else: print("input is not valid")

list1 = []
list2 = []

for i in range(1,101):
    if i % 2 == 0:
        list1.append(i)
    else:
        list2.append(i)

print(list1)
print(list2)

with open('data.txt', 'w') as file:
    file.write('Hello, Python!')

with open('data.txt', 'r') as file:
    data = file.read()
    print(data)