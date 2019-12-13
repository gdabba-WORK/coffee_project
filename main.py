from db_connection.coffee_init_service import DbInit
from db_connection.read_ddl import read_ddl_file


def read_ddl_file_test():
    db = read_ddl_file()

    # print(db)
    for key, value in db.items():
        if key != 'sql' and key !='trigger' and key != 'procedure' and key != 'backup' and key != 'restore':
            print("[{}] = {}".format(key, value))
        else:
            print("[{}]".format(key))
            for k, v in value.items():
                print("\t[{}]\n\t\t{}".format(k, v))

if __name__ == "__main__":
    # read_ddl_file_test()
    db = DbInit()
    db.service()
    # db.backup()
    # db.restore()
