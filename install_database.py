import database_operations

#  This program is used to create a database within an existing MySql server.
#  This module will load data stored in an offline CSV file into a MySQL database.
#  MySql server connection information is contained within the "database_operations" Module
#  
#  This module will delete all data currently stored within database tables when run
#  This is intended to ensure duplicate entries are not loaded into the database 

def main():   
    ##create the database and database table
    db_interface=database_operations.DB_Parser()
    db_interface.func_create_database()
    db_interface.func_purge_database()
    db_interface.func_create_table()   
    db_interface.func_load_csv_into_database()
#    db_interface.test_database()
if __name__ == "__main__":    
        main()
        
