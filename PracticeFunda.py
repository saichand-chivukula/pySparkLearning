import sys
import time as t
from datetime import date
#if we want to import specific class from the module we use the above format but if we want to use complete module which is a burden to the system we directly use the import statement

print(t.time())
print(date.today())
#print("name of this module is ", __name__)
# __name__ is a global variable, if we directly run the file then the global variable is set to __main__
# But if we run it or have it indirectly then the value of __name__ is set to the name of the file or module

if __name__ == "__main__":
    print("Executed when invoked directly")
else:
    print("Invoked indirectly")

