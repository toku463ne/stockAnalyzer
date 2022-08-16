import __init__
import db.inittables as inittables
import env

print("Preparing tables in test_db")
inittables.initTables()
print("Completed")
