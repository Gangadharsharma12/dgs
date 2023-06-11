import sys
# import time
# class bank:
#     name = "state bank of India"
#     acc = "XXXXXXXX595"
#     def __init__(self,minimum_balance = 1000,avl_bal = 5000):
#         self.Minimum = minimum_balance
#         self.Avl = avl_bal
#
#     def deposit(self, amount):
#         self.Amount = amount
#         self.Avl += self.Amount
#         op = input("Do you want to display the available balance[Yes | No]").lower()
#         if op == "yes":
#             self.credit()
#
#     def withdraw(self, amount):
#         self.Amount = amount
#         if self.Avl - self.Amount < self.Minimum:
#             print("sorry the minimum balance should be atleast 1000 RS")
#         else:
#             self.Avl -= self.Amount
#             print("please wait")
#             time.sleep(5)
#             print("please collect your cash")
#             op1 = input("Do you want to display the available balance[Yes | No]").lower()
#             if op1 == "yes":
#                 self.debit()
#
#     def bal(self):
#         print(f"Your current account balance is {self.Avl}")
#
#     def pin(self):
#         attempts = 3
#         print(f"Hello you have {attempts} attempts to enter correct pin:")
#         while attempts > 0:
#             pc = int(input("please enter your old pin:"))
#             if pc == 4235:
#                 pc1 = int(input("please enter your new pin:"))
#                 print("your pin has been changed successfully")
#                 break
#             else:
#                 attempts -= 1
#                 if attempts > 1:
#                     print(f"sorry,Wrong pin.you have {attempts} attempts left")
#                 elif attempts == 1:
#                     print(f"sorry,Wrong pin.you have {attempts} attempt left")
#                 else:
#                     print("You dont have any attempts left")
#
#         else:
#             print("Your card is temporarly blocked for 24 Hours.please try after 24 hours. For more details please contact toll free number 1234123423.Thank you")
#             sys.exit()
#
#
#     def leave(self):
#         ch = input("Do you want to display the available balance before leaving [yes | no]").lower()
#         if ch == "yes":
#             self.bal()
#             print("Thank you for choosing our banking services.Have a great day")
#             sys.exit()
#
#         else:
#             print("Thank you for choosing our banking services.Have a great day")
#             sys.exit()
#
#     def credit(self):
#         print(f"your account {bank.acc} has been credited with INR {self.Amount} and your current account balance is {self.Avl}")
#
#     def debit(self):
#             print(f"your account {bank.acc} has been debited with INR {self.Amount} and your current account balance is {self.Avl}")
#
#
# b = bank()
# print(f"Hello welcome to {bank.name} services")
# n = input("please enter your name:")
# print(f"Hello {n}", end=" ")
# print("please insert your card\nReading card...")
# time.sleep(5)
# while True:
#     print()
#     print("Please select your operation from below options:")
#     print("1.Deposit\n2.Withdraw\n3.Balance enquiry\n4.Pin change\n5.exit")
#     print()
#     choice = int(input("please select your choice:"))
#     if choice == 1:
#         amt = int(input("Please enter the amount to deposit:"))
#         if amt <= 0:
#             print("invalid amount to deposit")
#         else:
#             b.deposit(amt)
#
#
#     elif choice == 2:
#         amt = int(input("please enter the amount to withdraw:"))
#         b.withdraw(amt)
#
#     elif choice == 3:
#         b.bal()
#
#     elif choice == 4:
#         b.pin()
#
#     elif choice == 5:
#             b.leave()
#     else:
#         print("please select a valid option")